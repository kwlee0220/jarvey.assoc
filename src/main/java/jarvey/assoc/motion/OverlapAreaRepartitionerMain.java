package jarvey.assoc.motion;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.assoc.OverlapArea;
import jarvey.assoc.OverlapAreaRegistry;
import utils.UnitUtils;
import utils.func.Funcs;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class OverlapAreaRepartitionerMain {
	private static final Logger s_logger = LoggerFactory.getLogger(OverlapAreaRepartitionerMain.class.getPackage().getName());

	private static final String APP_ID = "jarvey-assoc-motion-repartition";
	private static final String DEFAULT_TOPIC_INPUT = "node-tracks";
	private static final String DEFAULT_TOPIC_OUTPUT = "overlap-areas";
	
	private static final String DEFAULT_POLL_TIMEOUT = "10s";
	/**
	 * 한번에 가져올 수 있는 최소 사이즈로, 만약 가져오는 데이터가 지정한 사이즈보다 작으면
	 * 요청에 응답하지 않고, 데이터가 누적될 때 까지 기다린다.
	 */
	private static final String DEFAULT_FETCH_MIN_BYTE = "64b";
	/**
	 * 한번에 가져올 수 있는 최대 데이터 사이즈.
	 */
	private static final String DEFAULT_FETCH_MAX_BYTE = "32mb";
	/**
	 * 설정된 데이터보다 데이터 양이 적은 경우 요청에 응답을 기다리는 최대시간을 설정.
	 */
	private static final String DEFAULT_FETCH_MAX_WAIT_MS = "10s";
	/**
	 * 컨슈머가 하트비트를 보냄에도 불구하고, poll을 하지 않으면 장애이기 때문에 poll 주기를
	 * 설정하여 장애를 판단하는데 사용한다. 해당 옵션보다 poll 주기가 길었을 경우
	 * 컨슈머 그룹에서 제외한 후, 다른 컨슈머가 해당 파티션을 처리할 수 있도록 한다.
	 */
	private static final String DEFAULT_MAX_POLL_INTERVAL = "30s";
	/**
	 * 폴링루프에서 이뤄지는 한건의 KafkaConsumer.poll() 메소드에 대한 최대 레코드수를 조정한다. 
	 */
	private static final String DEFAULT_MAX_POLL_RECORDS = String.valueOf(128 * 4); 
	/**
	 * 브로커에서 각 파티션 별로 최대로 반환할 수 있는 바이트 수. 
	 */
	private static final String DEFAULT_MAX_PARTITION_FETCH_BYTES = "256kb"; 
	/**
	 * Environment variables:
	 * <dl>
	 * 	<dt>KAFKA_GROUP_ID_CONFIG</dt>
	 * 	<dd>Consumer group 식별자.</dd>
	 * 
	 * 	<dt>KAFKA_BOOTSTRAP_SERVERS_CONFIG</dt>
	 * 	<dd>Kafka broker 접속 주소 리스트.</dd>
	 * 
	 * 	<dt>JARVEY_INPUT_TOPIC</dt>
	 * 	<dd>Repartition 대상 입력 topic 이름.</dd>
	 * 
	 * 	<dt>JARVEY_OUTPUT_TOPIC</dt>
	 * 	<dd>Repartition 결과 출력 topic 이름.</dd>
	 * </dl>
	 * @param args
	 * @throws Exception
	 */
	public static void main(String... args) throws Exception {
		Map<String,String> envs = System.getenv();
		
		OverlapAreaRegistry areaRegistry = new OverlapAreaRegistry();
		OverlapArea etriTestbed = new OverlapArea("etri_testbed");
		etriTestbed.addOverlap("etri:04", "etri:05");
		etriTestbed.addOverlap("etri:04", "etri:06");
		etriTestbed.addOverlap("etri:05", "etri:06");
		etriTestbed.addOverlap("etri:05", "etri:07");
		areaRegistry.add("etri_testbed", etriTestbed);
		
		String inputTopic = Funcs.asNonNull(envs.get("JARVEY_INPUT_TOPIC"), DEFAULT_TOPIC_INPUT);
		s_logger.info("use the input topic: {}", inputTopic);
		String outputTopic = Funcs.asNonNull(envs.get("JARVEY_OUTPUT_TOPIC"), DEFAULT_TOPIC_OUTPUT);
		s_logger.info("use the output topic: {}", outputTopic);

		String pollTimeoutStr = envs.getOrDefault("KAFKA_POLL_TIMEOUT", DEFAULT_POLL_TIMEOUT);
		s_logger.info("use KAFKA_POLL_TIMEOUT: {}", pollTimeoutStr);
		Duration pollTimeout = Duration.ofMillis(UnitUtils.parseDuration(pollTimeoutStr));

		Properties kafkaConsumerProps = buildKafkaConsumerProperties(envs);
		final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(kafkaConsumerProps);
		
		Properties kafkaProducerProps = buildKafkaProducerProperties(envs);
		final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(kafkaProducerProps);
		
		OverlapAreaRepartitioner exporter = new OverlapAreaRepartitioner(consumer, producer,
																		inputTopic, outputTopic,
																		areaRegistry,pollTimeout);

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
		
		exporter.run();
	}
	
	private static Properties buildKafkaConsumerProperties(Map<String,String> environs) {
		Properties props = new Properties();
		
		String kafkaServers = environs.getOrDefault("KAFKA_BOOTSTRAP_SERVERS_CONFIG", "localhost:9092");
		s_logger.info("use the KafkaServers: {}", kafkaServers);
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);

		String appId = environs.getOrDefault("KAFKA_GROUP_ID_CONFIG", APP_ID);
		s_logger.info("use Kafka group id: '{}'", appId);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, appId);
		
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		
//		String fetchMinBytes = environs.getOrDefault("KAFKA_FETCH_MIN_BYTES_CONFIG", DEFAULT_FETCH_MIN_BYTE);
//		s_logger.info("use FETCH_MIN_BYTES_CONFIG: {}", fetchMinBytes);
//		props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, (int)UnitUtils.parseByteSize(fetchMinBytes));
//		
//		String fetchMaxBytes = environs.getOrDefault("KAFKA_FETCH_MAX_BYTES_CONFIG", DEFAULT_FETCH_MAX_BYTE);
//		s_logger.info("use FETCH_MAX_BYTES_CONFIG: {}", fetchMaxBytes);
//		props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, (int)UnitUtils.parseByteSize(fetchMaxBytes));
//		
//		String fetchMaxWaitMillis = environs.getOrDefault("KAFKA_FETCH_MAX_WAIT_MS_CONFIG",
//															DEFAULT_FETCH_MAX_WAIT_MS);
//		s_logger.info("use FETCH_MAX_WAIT_MS_CONFIG: {}", fetchMaxWaitMillis);
//		props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, (int)UnitUtils.parseDuration(fetchMaxWaitMillis));
//
//		String maxPollIntvl = environs.getOrDefault("KAFKA_MAX_POLL_INTERVAL_MS_CONFIG",
//													DEFAULT_MAX_POLL_INTERVAL);
//		s_logger.info("use MAX_POLL_INTERVAL_MS_CONFIG: {}", maxPollIntvl);
//		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int)UnitUtils.parseDuration(maxPollIntvl));

		String maxPollRecords = environs.getOrDefault("KAFKA_MAX_POLL_RECORDS",
													DEFAULT_MAX_POLL_RECORDS);
		s_logger.info("use KAFKA_MAX_POLL_RECORDS: {}", maxPollRecords);
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, (int)UnitUtils.parseDuration(maxPollRecords));

		String maxPartFetchInBytes = environs.getOrDefault("KAFKA_MAX_PARTITION_FETCH_BYTES",
															DEFAULT_MAX_PARTITION_FETCH_BYTES);
		s_logger.info("use KAFKA_MAX_PARTITION_FETCH_BYTES: {}", maxPartFetchInBytes);
		props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, (int)UnitUtils.parseByteSize(maxPartFetchInBytes));
		
		return props;
	}
	
	private static Properties buildKafkaProducerProperties(Map<String,String> environs) {
		Properties props = new Properties();
		
		String kafkaServers = environs.getOrDefault("KAFKA_BOOTSTRAP_SERVERS_CONFIG", "localhost:9092");
		s_logger.info("use the KafkaServers: {}", kafkaServers);
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		
		return props;
	}
}
