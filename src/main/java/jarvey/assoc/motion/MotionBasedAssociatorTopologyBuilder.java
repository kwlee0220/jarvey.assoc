package jarvey.assoc.motion;


import java.io.File;
import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.UnitUtils;
import utils.UsageHelp;
import utils.func.Either;

import jarvey.assoc.AssociationClosure;
import jarvey.assoc.BinaryAssociation;
import jarvey.assoc.OverlapArea;
import jarvey.assoc.OverlapAreaRegistry;
import jarvey.streams.KafkaParameters;
import jarvey.streams.MockKeyValueStore;
import jarvey.streams.TrackTimestampExtractor;
import jarvey.streams.model.GlobalTrack;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.serialization.json.GsonUtils;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="motion-based association",
			parameterListHeading = "Parameters:%n",
			optionListHeading = "Options:%n",
			description="Motion-based NodeTrack association")
final class MotionBasedAssociatorTopologyBuilder implements Runnable {
	private static final Logger s_logger = LoggerFactory.getLogger(MotionBasedAssociatorMain.class);

	private static final String APPLICATION_ID = "motion-associator";
	private static final String TOPIC_NODE_TRACKS = "node-tracks-repartition";
	private static final String TOPIC_GLOBAL_TRACKS = "global-tracks-overlap-tentative";
	private static final String TOPIC_MOTION_ASSOCIATIONS = "motion-associations";
	
	private static final TrackTimestampExtractor TS_EXTRACTOR = new TrackTimestampExtractor();
	private static final String STORE_BINARY_ASSOCIATIONS = "binary-associations";
	private static final Duration DEFAULT_ASSOCIATION_INTERVAL = Duration.ofSeconds(1);
	private static final double DEFAULT_TRACK_DISTANCE = UnitUtils.parseLengthInMeter("5m");
	
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	@Mixin private KafkaParameters m_kafkaParams;

	@Option(names={"--overlap-area"}, paramLabel="overlap-area-descriptor",
			description="overlap area description file path.")
	private String m_overlapAreaFilePath = "overlap_areas.yaml";

	@Option(names={"--input"}, paramLabel="topic-name", description="input topic name")
	private String m_inputTopic = TOPIC_NODE_TRACKS;

	@Option(names={"--output-associations"}, paramLabel="topic-name",
					description="output association topic. (default: motion-associations)")
	private String m_outputAssocTopic = TOPIC_MOTION_ASSOCIATIONS;

	@Option(names={"--output-tracks"}, paramLabel="topic-name",
					description="output global-track topic. (default: global-tracks-overlap-tentative")
	private String m_outputTrackTopic = TOPIC_GLOBAL_TRACKS;
	
	@Option(names={"--assoc-interval"}, paramLabel="interval",
			description="Motion-based association interval (default: 1s).")
	public void setAssociationInterval(String durationStr) {
		m_assocInterval = Duration.ofMillis(UnitUtils.parseDuration(durationStr));
	}
	private Duration m_assocInterval = DEFAULT_ASSOCIATION_INTERVAL;
	
	@Option(names={"--max-track-distance"}, paramLabel="distance",
			description="maximun distance difference allowance for a same track (default: 5m).")
	public void setMaxTrackDistance(String distStr) {
		m_maxTrackDistance = UnitUtils.parseLengthInMeter(distStr);
	}
	private double m_maxTrackDistance = DEFAULT_TRACK_DISTANCE;

	@Option(names={"--use-mock-store"}, description="Use mocking state store (for test).")
	private boolean m_useMockStateStore = false;
	
	private OverlapAreaRegistry m_areaRegistry;
	private final MotionBasedAssociatorContext m_mbaContext = new MotionBasedAssociatorContext();
	
	public MotionBasedAssociatorTopologyBuilder setAutoOffsetReset(AutoOffsetReset reset) {
		m_kafkaParams.setAutoOffsetReset(reset.toString());
		return this;
	}
	
	@Override
	public void run() {
		try {
			m_areaRegistry = OverlapAreaRegistry.load(new File(m_overlapAreaFilePath));
			
			if ( m_kafkaParams.getApplicationId() == null ) {
				m_kafkaParams.setApplicationId(APPLICATION_ID);
			}
			
			Topology topology = build();
			
			if ( s_logger.isInfoEnabled() ) {
				s_logger.info("use Kafka servers: {}", m_kafkaParams.getBootstrapServers());
				s_logger.info("use Kafka application: {}", m_kafkaParams.getApplicationId());
			}
			
			Properties props = m_kafkaParams.toStreamProperties();
			KafkaStreams streams = new KafkaStreams(topology, props);
			Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
			
			streams.start();
		}
		catch ( Exception e ) {
			throw new RuntimeException(e);
		}
	}
	
	@SuppressWarnings("deprecation")
	private Topology build() {
		StreamsBuilder builder = new StreamsBuilder();
		
		String[] binaryAssocStoreNames;
		if ( m_useMockStateStore ) {
			MockKeyValueStore<TrackletId,BinaryAssociationStore.Record> kvStore
						= new MockKeyValueStore<>(STORE_BINARY_ASSOCIATIONS,
													GsonUtils.getSerde(TrackletId.class),
													GsonUtils.getSerde(BinaryAssociationStore.Record.class));
			m_mbaContext.addMockKeyValueStore(STORE_BINARY_ASSOCIATIONS, kvStore);
			binaryAssocStoreNames = new String[]{};
		}
		else {
			builder.addStateStore(Stores.keyValueStoreBuilder(
												Stores.persistentKeyValueStore(STORE_BINARY_ASSOCIATIONS),
												GsonUtils.getSerde(TrackletId.class),
												GsonUtils.getListSerde(BinaryAssociation.class)));
			binaryAssocStoreNames = new String[]{STORE_BINARY_ASSOCIATIONS};
		}
		
		KStream<String,NodeTrack> validNodeTracks =
			builder
				.stream(m_inputTopic,
						Consumed.with(Serdes.String(), GsonUtils.getSerde(NodeTrack.class))
								.withName("from-node-tracks")
								.withTimestampExtractor(TS_EXTRACTOR)
								.withOffsetResetPolicy(m_kafkaParams.getAutoOffsetReset()))
				.filter(this::withAreaDistance, Named.as("filter-valid-node-tracks"));
		
		validNodeTracks
			.flatTransform(this::createBinaryAssociator,
									Named.as("binary-association"), binaryAssocStoreNames)
			.flatTransformValues(this::createClosureBuilder, Named.as("closure-builder"))
			.mapValues(AssociationClosure::toDao)
			.to(m_outputAssocTopic,
				Produced.with(Serdes.String(), GsonUtils.getSerde(AssociationClosure.DAO.class))
						.withName("to-motion-associations"));
		
		validNodeTracks
			.flatMapValues(new GlobalTrackGenerator(m_mbaContext), Named.as("generate-global-tracks"))
			.to(m_outputTrackTopic,
				Produced.with(Serdes.String(), GsonUtils.getSerde(GlobalTrack.class))
						.withName("to-global-tracks"));
		
		return builder.build();
	}
	
	private BinaryTrackAssociator createBinaryAssociator() {
		BinaryTrackAssociator ba = new BinaryTrackAssociator(m_assocInterval, m_maxTrackDistance,
														STORE_BINARY_ASSOCIATIONS, m_useMockStateStore);
		m_mbaContext.setBinaryTrackAssociator(ba);
		return ba;
	}
	
	private AssociationClosureBuilder createClosureBuilder() {
		AssociationClosureBuilder builder = new AssociationClosureBuilder(m_mbaContext);
		m_mbaContext.setAssociationClosureBuilder(builder);
		return builder;
	}
	
	private boolean withAreaDistance(String areaId, NodeTrack track) {
		if ( areaId == null ) {
			return false;
		}
		
		if ( track.isDeleted() ) {
			return true;
		}
		
		OverlapArea area = m_areaRegistry.get(areaId);
		double threshold = area.getDistanceThreshold(track.getNodeId());
		return track.getDistance() <= threshold;
	}
	
	private static Predicate<String,Either<AssociationClosure,GlobalTrack>> s_toAssociationBranch
		= new Predicate<String,Either<AssociationClosure,GlobalTrack>>() {
			@Override
			public boolean test(String areaId, Either<AssociationClosure, GlobalTrack> either) {
				return either.isLeft();
			}
		};
	
	private static Predicate<String,Either<AssociationClosure,GlobalTrack>> s_toGlobalTrackBranch
		= new Predicate<String,Either<AssociationClosure,GlobalTrack>>() {
			@Override
			public boolean test(String areaId, Either<AssociationClosure, GlobalTrack> either) {
				return either.isRight();
			}
		};
}
