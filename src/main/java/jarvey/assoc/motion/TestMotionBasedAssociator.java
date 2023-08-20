package jarvey.assoc.motion;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.UnitUtils;
import utils.func.Either;
import utils.func.Unchecked;

import jarvey.assoc.AssociationClosure;
import jarvey.assoc.OverlapArea;
import jarvey.assoc.OverlapAreaRegistry;
import jarvey.streams.model.GlobalTrack;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.serialization.json.GsonUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestMotionBasedAssociator {
	private static final Logger s_logger = LoggerFactory.getLogger(TestMotionBasedAssociator.class.getPackage().getName());

	private static final String TOPIC_INPUT = "node-tracks";
	private static final String TOPIC_NON_OVERLAP_AREAS = "non_overlap_areas";
	private static final String TOPIC_MOTION_ASSOCIATION = "motion-associations";
	private static final String STORE_BINARY_ASSOCIATIONS = "binary_associations";
	
	private static final Duration WINDOW_SIZE = Duration.ofSeconds(1);
	private static final Duration ADVANCE_TIME = Duration.ofSeconds(1);
	private static final Duration GRACE_TIME = Duration.ofSeconds(1);
	private static final double DEFAULT_TRACK_DISTANCE = 5;
	
	public static void main(String... args) throws Exception {
		Map<String,String> envs = System.getenv();
		
		String topic = TOPIC_INPUT;
		Duration pollTimeout = Duration.ofMillis(1000);

		OverlapAreaRegistry areas = OverlapAreaRegistry.load(new File("overlap_areas.yaml"));
		
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_global_locator");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, (int)UnitUtils.parseDuration("10s"));
		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int)UnitUtils.parseDuration("5m"));

		final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
		
		Properties producerProps = new Properties();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);
		
		Thread mainThread = Thread.currentThread();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					consumer.wakeup();
					mainThread.join();
				}
				catch ( InterruptedException e ) {
					s_logger.error("interrupted", e);
				}
			}
		});
		
		driver(consumer, topic, pollTimeout, areas, producer);
	}
	
	private static class TopicPublisher<T> implements BiConsumer<String, T> {
		private final KafkaProducer<String, byte[]> m_producer;
		private final String m_topic;
		private final Serializer<T> m_valueSerde;
		
		TopicPublisher(KafkaProducer<String, byte[]> producer, String topic, Serde<T> valueSerde) {
			m_producer = producer;
			m_topic = topic;
			m_valueSerde = valueSerde.serializer();
		}

		@Override
		public void accept(String key, T value) {
			byte[] bytes = m_valueSerde.serialize(m_topic, value);
			ProducerRecord<String,byte[]> closureRecord = new ProducerRecord<>(m_topic, key, bytes);
			m_producer.send(closureRecord);
		}
		
	}
	
	public static void driver(KafkaConsumer<String, byte[]> consumer, String topic, Duration pollTimeout,
								OverlapAreaRegistry registry, KafkaProducer<String, byte[]> producer) {
		MotionBasedAssociateTransformer associator
			= new MotionBasedAssociateTransformer(registry, WINDOW_SIZE, DEFAULT_TRACK_DISTANCE,
														STORE_BINARY_ASSOCIATIONS);
		
		TopicPublisher<AssociationClosure.DAO> assocPublisher
			= new TopicPublisher<>(producer, TOPIC_MOTION_ASSOCIATION,
									GsonUtils.getSerde(AssociationClosure.DAO.class));
		TopicPublisher<GlobalTrack> gtrackPublisher
			= new TopicPublisher<>(producer, "global-tracks", GsonUtils.getSerde(GlobalTrack.class));
		try {
			consumer.subscribe(Arrays.asList(topic));
			
			Serde<NodeTrack> serde = GsonUtils.getSerde(NodeTrack.class);
			Deserializer<NodeTrack> deser = serde.deserializer();
			
			long lastOffset = -1;
			while ( true ) {
				ConsumerRecords<String,byte[]> records = consumer.poll(pollTimeout);
				for ( ConsumerRecord<String, byte[]> record: records ) {
					if ( lastOffset >= record.offset() ) {
						continue;
					}
					NodeTrack track = deser.deserialize(record.topic(), record.value());
//					System.out.println(track);
					
					OverlapArea area = registry.findByNodeId(record.key()).getOrNull();
					String areaId = area.getId();
					
					if ( !track.isDeleted()
						&& track.getDistance() > area.getDistanceThreshold(track.getNodeId()) ) {
						continue;
					}
					
					for ( KeyValue<String,Either<AssociationClosure,GlobalTrack>> kv: associator.transform(areaId, track) ) {
						String key = kv.key;
						Either<AssociationClosure,GlobalTrack> either = kv.value;
						
						if ( either.isLeft() ) {
//							assocPublisher.accept(key, either.getLeft().toDao());
							System.out.printf("%s: %s%n", key, either.getLeft());
						}
						else {
//							gtrackPublisher.accept(key, either.getRight());
//							System.out.printf("%s: %s%n", key, either.getRight());
						}
					}
				}
			}
		}
		catch ( WakeupException ignored ) { }
		catch ( Exception e ) {
			s_logger.error("Unexpected error", e);
		}
		finally {
			Unchecked.runOrIgnore(consumer::close);
			Unchecked.runOrIgnore(producer::close);
		}
	}
}
