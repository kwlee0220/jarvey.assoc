package jarvey.assoc.feature;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;

import utils.jdbc.JdbcParameters;
import utils.jdbc.JdbcProcessor;

import jarvey.streams.model.Range;
import jarvey.streams.node.TrackFeature;
import jarvey.streams.updatelog.KeyedUpdateIndex;
import jarvey.streams.updatelog.KeyedUpdateLogs;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TestFeature {
	@SuppressWarnings("resource")
	public static void main(String... args) throws Exception {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass().getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.ByteArray().deserializer().getClass().getName());
		
		Deserializer<TrackFeature> deser = TrackFeatureSerde.s_serde.deserializer();
		
		JdbcParameters jdbcParams = new JdbcParameters();
		jdbcParams.jdbcLoc("postgresql:localhost:5432:dna:urc2004:dna");
		JdbcProcessor jdbc = jdbcParams.createJdbcProcessor();
		
		KeyedUpdateLogs<TrackFeature> tFeatureIndexes
			= new KeyedUpdateLogs<>(jdbc, "track_features_index", props, "track-features", deser);
		
//		Range<Long> range = Range.atMost(23500L);
//		Range<Long> range = Range.atLeast(20000L);
		Range<Long> range = Range.between(20000L, 37500L);
		for ( KeyedUpdateIndex idx: tFeatureIndexes.listKeyedUpdateIndex(range) ) {
			System.out.println(idx);
		}
		
		for ( KeyValue<String,TrackFeature> kv: tFeatureIndexes.streamOfKeys(Arrays.asList("etri:05[4]", "etri:07[4]")) ) {
			System.out.println(kv.key + ": " + kv.value);
		}
	}
}
