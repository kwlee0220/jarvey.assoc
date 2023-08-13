package jarvey.assoc.motion;

import java.io.File;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.assoc.OverlapAreaRegistry;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MotionBasedAssociatorMain {
	private static final Logger s_logger = LoggerFactory.getLogger(MotionBasedAssociatorMain.class);
	
	public static void main(String... args) throws Exception {
		Map<String,String> envs = System.getenv();
		
		String appId = envs.getOrDefault("KAFKA_APPLICATION_ID_CONFIG", "motion-associator");
		String kafkaServers = envs.getOrDefault("KAFKA_BOOTSTRAP_SERVERS_CONFIG", "localhost:9092");
		String areaFilePath = envs.getOrDefault("JARVEY_OVERLAP_AREA_FILE", "overlap_areas.yaml");
		double trackDistance = UnitUtils.parseLengthInMeter(envs.getOrDefault("JARVEY_TRACK_DISTANCE", "5"));
		AutoOffsetReset offsetReset
					= AutoOffsetReset.valueOf(envs.getOrDefault("JARVEY_AUTO_OFFSET_RESET", "LATEST"));
		
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("use Kafka servers: {}", kafkaServers);
			s_logger.info("use Kafka topic: {}={}", "APPLICATION_ID", appId);
		}
		
		OverlapAreaRegistry areaRegistry = OverlapAreaRegistry.load(new File(areaFilePath));
		Topology topology = new MotionBasedAssociatorTopologyBuilder(areaRegistry)
																.withOffsetReset(offsetReset)
																.withinTrackDistance(trackDistance)
																.build();
		
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ByteArraySerde.class);
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
//		config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		
		KafkaStreams streams = new KafkaStreams(topology, config);
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		
		streams.start();
	}
}
