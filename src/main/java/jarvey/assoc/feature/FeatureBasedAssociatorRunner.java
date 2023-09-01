package jarvey.assoc.feature;

import java.util.Properties;
import java.util.Set;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.CSV;
import utils.UsageHelp;
import utils.func.Either;
import utils.jdbc.JdbcParameters;
import utils.jdbc.JdbcProcessor;

import jarvey.assoc.AssociationCollection;
import jarvey.assoc.feature.Processors.ContextSetter;
import jarvey.streams.KafkaAdmins;
import jarvey.streams.KafkaParameters;
import jarvey.streams.model.AssociationClosure;
import jarvey.streams.model.BinaryAssociation;
import jarvey.streams.model.JarveySerdes;
import jarvey.streams.model.TrackletDeleted;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.node.TrackFeature;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
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
final class FeatureBasedAssociatorRunner implements Runnable {
	private static final Logger s_logger = LoggerFactory.getLogger(FeatureBasedAssociatorRunner.class);

	private static final String APPLICATION_ID = "feature-associator";
	private static final String TOPIC_TRACK_FEATURES = "track-features";
	private static final String TOPIC_GLOBAL_TRACKS = "global-tracks-overlap-tentative";
	private static final String TOPIC_FEATURE_ASSOCIATIONS = "feature-associations";
	
	private static final String STORE_BINARY_ASSOCIATIONS = "binary-associations";
	private static final double DEFAULT_MIN_SCORE = 0.3;
	private static final double DEFAULT_TOP_PERCENT = 0.2;
	
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	@Mixin private KafkaParameters m_kafkaParams;
	@Mixin private JdbcParameters m_jdbcParams;

	@Option(names={"--input"}, paramLabel="topic-name", description="input topic name")
	private String m_inputTopic = TOPIC_TRACK_FEATURES;

	@Option(names={"--output-associations"}, paramLabel="topic-name",
					description="output association topic. (default: feature-associations)")
	private String m_outputAssocTopic = TOPIC_FEATURE_ASSOCIATIONS;

	@Option(names={"--output-tracks"}, paramLabel="topic-name",
					description="output global-track topic. (default: global-tracks-overlap-tentative")
	private String m_outputTrackTopic = TOPIC_GLOBAL_TRACKS;
	
	@Option(names={"--min-association-score"}, paramLabel="score",
			description="maximun distance difference allowance for a same track (default: 0.3).")
	private double m_minBinaryAssociationScore = DEFAULT_MIN_SCORE;
	
	@Option(names={"--top-percent"}, paramLabel="percentage",
			description="percentage for top-k score (default: 0.2).")
	private double m_topPercent = DEFAULT_TOP_PERCENT;

	@Option(names={"--use-mock-store"}, description="Use mocking state store (for test).")
	private boolean m_useMockStateStore = false;

	private Set<String> m_listeningNodes;
	public Set<String> getListeningNodes() {
		return m_listeningNodes;
	}
	@Option(names={"--listen"}, paramLabel="node-names", description="listening node names")
	public void setListeningNodes(String names) {
		m_listeningNodes = CSV.parseCsv(names).toSet();
	}
	
	public FeatureBasedAssociatorRunner setAutoOffsetReset(AutoOffsetReset reset) {
		m_kafkaParams.setAutoOffsetReset(reset.toString());
		return this;
	}
	
	private JdbcProcessor m_jdbc;
	private FeatureBasedAssociationContext m_context;
	private AssociationCollection<AssociationClosure> m_collection;
	private AssociationClosureBuilder m_associationExpander;
	private FinalAssociationSelector m_finalSelector;
	
	@Override
	public void run() {
		try {
			if ( m_kafkaParams.getApplicationId() == null ) {
				m_kafkaParams.setApplicationId(APPLICATION_ID);
			}
			
			m_jdbc = m_jdbcParams.createJdbcProcessor();
			m_context = new FeatureBasedAssociationContext();
			m_collection = new AssociationCollection<>(false);
			m_associationExpander = new AssociationClosureBuilder(m_collection);
			m_finalSelector = new FinalAssociationSelector(m_context, m_listeningNodes, m_collection);
			
			Topology topology = build();
			
			if ( s_logger.isInfoEnabled() ) {
				s_logger.info("use Kafka servers: {}", m_kafkaParams.getBootstrapServers());
				s_logger.info("use Kafka application: {}", m_kafkaParams.getApplicationId());
			}
			
			Properties props = m_kafkaParams.toStreamProperties();
			KafkaStreams streams = new KafkaStreams(topology, props);
			Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

			try {
				KafkaAdmins admin = new KafkaAdmins(m_kafkaParams.getBootstrapServers());
				admin.deleteConsumerGroup(m_kafkaParams.getApplicationId());
			}
			catch ( Exception ignored ) { }
			
			streams.start();
		}
		catch ( Exception e ) {
			throw new RuntimeException(e);
		}
	}
	
	@SuppressWarnings("deprecation")
	private Topology build() {
		StreamsBuilder builder = new StreamsBuilder();
		
		String[] binaryAssocStoreNames = new String[]{};
		if ( !m_useMockStateStore ) {
			builder.addStateStore(Stores.keyValueStoreBuilder(
												Stores.persistentKeyValueStore(STORE_BINARY_ASSOCIATIONS),
												JarveySerdes.TrackletId(),
												JarveySerdes.BinaryAssociation()));
			binaryAssocStoreNames = new String[]{STORE_BINARY_ASSOCIATIONS};
		}

		@SuppressWarnings("unchecked")
		KStream<String,Either<BinaryAssociation, TrackletDeleted>>[] branches
			= builder
				.stream(m_inputTopic,
						Consumed.with(Serdes.String(), Serdes.ByteArray()) 
								.withName("from-track-features")
								.withOffsetResetPolicy(m_kafkaParams.getAutoOffsetReset()))
				.filter(this::filterListeningNodes)
				.mapValues(this::toTrackFeature)
				.flatTransformValues(this::createContextSetter, binaryAssocStoreNames)
				.flatMapValues(createBinaryAssociationGenerator(), Named.as("binary-association"))
				.branch(this::isBinaryAssociation, this::isTrackletDeleted);
		
		branches[0]
			.mapValues(either -> either.getLeft())
			.filter((n,ba) -> ba.getScore() >= m_minBinaryAssociationScore)
			.flatMapValues(m_associationExpander, Named.as("closure-builder"))
			.print(Printed.toSysOut());
		
		branches[1]
			.mapValues(either -> either.getRight())
			.flatMapValues(m_finalSelector, Named.as("final-association"))
			.mapValues(AssociationClosure::toDao, Named.as("to-association-dao"))
//			.print(Printed.toSysOut());
			.to(m_outputAssocTopic,
				Produced.with(Serdes.String(), JarveySerdes.AssociationClosure())
						.withName("to-feature-associations"));
		
		builder
			.stream("node-tracks-repartition",
					Consumed.with(Serdes.String(), JarveySerdes.NodeTrack()) 
							.withName("from-node-tracks")
							.withOffsetResetPolicy(m_kafkaParams.getAutoOffsetReset()))
			.filter(this::filterListeningNodes)
			.mapValues(new GlobalTrackGenerator(m_collection), Named.as("to-global-track"))
//			.print(Printed.toSysOut());
			.to(m_outputTrackTopic,
				Produced.with(Serdes.String(), JarveySerdes.GlobalTrack())
						.withName("to-global-tracks"));
		
		return builder.build();
	}
	
	private BinaryTrackletAssociator createBinaryAssociationGenerator() {
		return new BinaryTrackletAssociator(m_context, m_jdbc, m_kafkaParams.toConsumerProperties(),
											m_topPercent);
	}
	
	private ContextSetter createContextSetter() {
		return new ContextSetter(m_context, STORE_BINARY_ASSOCIATIONS, m_useMockStateStore);
	}
	
	private boolean filterListeningNodes(String nodeId, byte[] bytes) {
		return m_listeningNodes.contains(nodeId);
	}
	
	private boolean filterListeningNodes(String nodeId, NodeTrack track) {
		return m_listeningNodes.contains(track.getNodeId());
	}
	
	private TrackFeature toTrackFeature(String nodeId, byte[] bytes) {
		return TrackFeatureSerde.s_deerializer.deserialize(m_inputTopic, bytes);
	}
	
	private boolean isBinaryAssociation(String nodeId, Either<BinaryAssociation, TrackletDeleted> either) {
		return either.isLeft();
	}
	private boolean isTrackletDeleted(String nodeId, Either<BinaryAssociation, TrackletDeleted> either) {
		return either.isRight();
	}
	
	@SuppressWarnings("deprecation")
	public static final void main(String... args) throws Exception {
		FeatureBasedAssociatorRunner cmd = new FeatureBasedAssociatorRunner();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				cmd.run();
			}
		}
		catch ( Throwable e ) {
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
}
