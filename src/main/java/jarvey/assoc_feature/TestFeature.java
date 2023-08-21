package jarvey.assoc_feature;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;

import com.google.common.primitives.Floats;

import jarvey.streams.node.TrackFeature;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TestFeature {
	public static void main(String... args) throws Exception {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass().getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.ByteArray().deserializer().getClass().getName());
		
		Deserializer<TrackFeature> deser = TrackFeatureSerde.s_serde.deserializer();
		
		KafkaConsumer<String,byte[]> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singleton("track-features"));
		
			for ( ConsumerRecord<String, byte[]> record: consumer.poll(Duration.ofSeconds(10)) ) {
				String key = record.key();
				byte[] bytes = record.value();
				
				TrackFeature tfeature = deser.deserialize(record.topic(), bytes);
				if ( tfeature.getFeature().length > 0 ) {
					List<Float> feature = Floats.asList(tfeature.getFeature());
					
					System.out.printf("key=%s, track_id=%s feature=%s%n", key, tfeature.getTrackId(), feature.subList(0, 5));
				}
				
			}
	}
}
