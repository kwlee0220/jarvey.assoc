package jarvey.assoc.motion;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.UnitUtils;
import utils.func.Unchecked;

import jarvey.assoc.OverlapAreaRegistry;
import jarvey.streams.HoppingWindowManager;
import jarvey.streams.model.GlobalTrack;
import jarvey.streams.model.NodeTrack;
import jarvey.streams.serialization.json.GsonUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestMotionBasedAssociator {
	private static final Logger s_logger = LoggerFactory.getLogger(TestMotionBasedAssociator.class.getPackage().getName());

	private static final String TOPIC_INPUT = "overlap-areas";
	private static final String TOPIC_NON_OVERLAP_AREAS = "non_overlap_areas";
	private static final String TOPIC_MOTION_ASSOCIATION = "motion_associations";
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
		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int)UnitUtils.parseDuration("300s"));

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
	
	public static void driver(KafkaConsumer<String, byte[]> consumer, String topic, Duration pollTimeout,
								OverlapAreaRegistry registry, KafkaProducer<String, byte[]> producer) {
		HoppingWindowManager windowMgr = HoppingWindowManager.ofWindowSize(WINDOW_SIZE);
		ChopNodeTracks chopper = new ChopNodeTracks(windowMgr);
		DropTooFarTracks dropper = new DropTooFarTracks(registry);
		AssociationClosureBuilder closureBuilder
				= new AssociationClosureBuilder(registry, DEFAULT_TRACK_DISTANCE);
		GlobalTrackGenerator glocator = new GlobalTrackGenerator(registry,
																closureBuilder.getClosureCollections());
		try {
			consumer.subscribe(Arrays.asList(topic));
			
			Serde<NodeTrack> serde = GsonUtils.getSerde(NodeTrack.class);
			Deserializer<NodeTrack> deser = serde.deserializer();
			
			Serde<GlobalTrack> gtrackSerde = GsonUtils.getSerde(GlobalTrack.class);
			Serializer<GlobalTrack> gtrackSer = gtrackSerde.serializer();
			
			Serde<AssociationClosure.DAO> closureSerde = GsonUtils.getSerde(AssociationClosure.DAO.class);
			Serializer<AssociationClosure.DAO> clSer = closureSerde.serializer();
			
			long lastOffset = -1;
			while ( true ) {
				ConsumerRecords<String,byte[]> records = consumer.poll(pollTimeout);
				for ( ConsumerRecord<String, byte[]> record: records ) {
					if ( lastOffset >= record.offset() ) {
						continue;
					}
					NodeTrack track = deser.deserialize(record.topic(), record.value());
//					System.out.println(track);
					
					for ( OverlapAreaTagged<List<NodeTrack>> bucket: chopper.apply(record.key(), track)) {
//						System.out.println(bucket);
						OverlapAreaTagged<List<NodeTrack>> closeBucket = dropper.apply(bucket);
						String areaId = closeBucket.areaId();
						
						for ( AssociationClosure cl: closureBuilder.transform(closeBucket) ) {
//							System.out.println("FINAL ASSOC: " + cl);
							System.out.println(cl);
							
							byte[] bytes = clSer.serialize(TOPIC_MOTION_ASSOCIATION, cl.toDao());
							ProducerRecord<String,byte[]> closureRecord
								= new ProducerRecord<>(TOPIC_MOTION_ASSOCIATION, areaId, bytes);
							producer.send(closureRecord);
						}
					}
					
					Iterable<GlobalTrack> gtracks = glocator.apply(record.key(), track);
					for ( GlobalTrack gtrack: gtracks ) {
//						System.out.println("\t" + gtrack);
						
						byte[] bytes = gtrackSer.serialize("global-tracks", gtrack);
						ProducerRecord<String,byte[]> gtrackRecord
										= new ProducerRecord<>("global-tracks", gtrack.getNodeId(), bytes);
						producer.send(gtrackRecord);
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
