package jarvey.assoc.motion;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.reflect.TypeToken;

import io.confluent.common.utils.TestUtils;
import jarvey.assoc.BinaryAssociation;
import jarvey.streams.model.TrackletId;
import jarvey.streams.serialization.json.GsonUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestKTable {
	private static final Logger s_logger = LoggerFactory.getLogger(TestKTable.class.getPackage().getName());

	private static final String TOPIC_INPUT = "overlap-areas";
	private static final String TOPIC_NON_OVERLAP_AREAS = "non_overlap_areas";
	private static final String TOPIC_MOTION_ASSOCIATION = "motion_associations";
	private static final String STORE_MOTION_ASSOCIATION = "motion_associations";
	
	private static final Duration WINDOW_SIZE = Duration.ofSeconds(1);
	private static final Duration ADVANCE_TIME = Duration.ofSeconds(1);
	private static final Duration GRACE_TIME = Duration.ofSeconds(1);
	private static final double DEFAULT_TRACK_DISTANCE = 5;
	
	public static void main(String... args) throws Exception {
		Type type = new TypeToken<List<BinaryAssociation>>(){}.getType();
		Type type2 = TypeToken.getParameterized(List.class, BinaryAssociation.class).getType();
		Serde<List<BinaryAssociation>> serde = GsonUtils.getListSerde(BinaryAssociation.class);
		
		BinaryAssociation ba = new BinaryAssociation(new TrackletId("etri:04", "1"), new TrackletId("etri:05", "1"),
								1.1, 10, 20);
		byte[] bytes = serde.serializer().serialize("", Arrays.asList(ba));
		List<BinaryAssociation> out = serde.deserializer().deserialize("", bytes);
		System.out.println(out);
		
		
//		List<String> lines = Files.readLines(new File("test.json"), Charset.defaultCharset());
//		String line = lines.get(0);
//		Type listType = new TypeToken<List<BinaryAssociation>>(){}.getType();
//		Object obj = GsonUtils.parseJson(line, listType);

		StreamsBuilder builder = new StreamsBuilder();
		builder
			.table("associator_motion-motion_associations-changelog",
					Consumed.with(AutoOffsetReset.EARLIEST)
							.with(Serdes.String(), GsonUtils.getListSerde(BinaryAssociation.class)))
			.toStream()
			.print(Printed.toSysOut());
		Topology topology = builder.build();
		
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "table");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, ByteArraySerde.class);
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ByteArraySerde.class);
		config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
//		config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		
		KafkaStreams streams = new KafkaStreams(topology, config);
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		
		streams.start();
	}
}
