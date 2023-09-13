package jarvey.assoc;


import java.util.Properties;
import java.util.Set;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import utils.UsageHelp;
import utils.jdbc.JdbcParameters;
import utils.jdbc.JdbcProcessor;

import jarvey.assoc.feature.FeatureAssociationParams;
import jarvey.assoc.feature.FeatureAssociationStreamBuilder;
import jarvey.assoc.motion.MotionAssociationParams;
import jarvey.assoc.motion.MotionAssociationStreamBuilder;
import jarvey.streams.KafkaAdmins;
import jarvey.streams.KafkaParameters;
import jarvey.streams.TrackTimestampExtractor;
import jarvey.streams.model.JarveySerdes;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.node.TrackFeature;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="track-association",
			parameterListHeading = "Parameters:%n",
			optionListHeading = "Options:%n",
			description="Motion-based NodeTrack association")
final class AssociatorRunner implements Runnable {
	private static final Logger s_logger = LoggerFactory.getLogger(AssociatorRunner.class);

	private static final String APPLICATION_ID = "associator";
	private static final TrackTimestampExtractor TS_EXTRACTOR = new TrackTimestampExtractor();
	
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	@Mixin private KafkaParameters m_kafkaParams;
	@Mixin private JdbcParameters m_jdbcParams;
	@Mixin private AssociationParams m_params;
	
	@Override
	public void run() {
		try {
			if ( m_kafkaParams.getApplicationId() == null ) {
				m_kafkaParams.setApplicationId(APPLICATION_ID);
			}
			
			OverlapAreaRegistry areaRegistry = m_params.getOverlapAreaRegistry();
			Set<String> listeningNodes = m_params.getListeningNodes();
			Properties consumerProps = m_kafkaParams.toConsumerProperties();
			JdbcProcessor jdbc = m_jdbcParams.createJdbcProcessor();
			AssociationCollection associations = new AssociationCollection("associations");
			Set<TrackletId> closedTracklets = Sets.newHashSet();
			AssociationCollection finalAssociations = new AssociationCollection("final-associations");
			
			StreamsBuilder builder = new StreamsBuilder();
			
			KStream<String,NodeTrack> nodeTracks =
				builder
					.stream(m_params.getNodeTracksTopic(),
							Consumed.with(Serdes.String(), JarveySerdes.NodeTrack())
									.withName("from-node-tracks")
									.withTimestampExtractor(TS_EXTRACTOR)
									.withOffsetResetPolicy(m_kafkaParams.getAutoOffsetReset()));
			
			MotionAssociationParams motionParams = new MotionAssociationParams();
			motionParams.setOverlapRegistry(areaRegistry);
			motionParams.setNodeTracksTopic(m_params.getNodeTracksTopic());
			motionParams.setAssociationsTopic(m_params.getAssociationsTopic());
			motionParams.setGlobalTracksTopic(m_params.getGlobalTracksTopic());

			MotionAssociationStreamBuilder motionBuilder
					= new MotionAssociationStreamBuilder(motionParams, jdbc, associations, closedTracklets,
														finalAssociations);
			motionBuilder.setGenerateGlobalTracks(false);
			motionBuilder.build(nodeTracks);
			
			FeatureAssociationParams featureParams = new FeatureAssociationParams();
			featureParams.setOverlapRegistry(areaRegistry);
			featureParams.setListeningNodes(listeningNodes);
			featureParams.setNodeTracksTopic(m_params.getNodeTracksTopic());
			featureParams.setAssociationsTopic(m_params.getAssociationsTopic());
			featureParams.setGlobalTracksTopic(m_params.getGlobalTracksTopic());
			featureParams.setListeningNodes(m_params.getListeningNodes());

			KStream<String,TrackFeature> trackFeatures =
				builder
					.stream(m_params.getTrackFeaturesTopic(),
							Consumed.with(Serdes.String(), JarveySerdes.TrackFeature())
									.withName("from-track-features")
									.withOffsetResetPolicy(m_kafkaParams.getAutoOffsetReset()));

			FeatureAssociationStreamBuilder featureBuilder
					= new FeatureAssociationStreamBuilder(featureParams, jdbc, consumerProps,
															associations, closedTracklets, finalAssociations);
			featureBuilder.setGenerateGlobalTracks(false);
			featureBuilder.build(nodeTracks, trackFeatures);

			GlobalTrackGenerator gtrackGen = new GlobalTrackGenerator(areaRegistry, listeningNodes,
																		associations, finalAssociations);
			nodeTracks
				.flatMap(gtrackGen, Named.as("generate-global-track"))
				.to(m_params.getGlobalTracksTopic(),
					Produced.with(Serdes.String(), JarveySerdes.GlobalTrack())
							.withName("to-global-tracks-tentative"));
			
			Topology topology = builder.build();
			
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
	public static final void main(String... args) throws Exception {
		AssociatorRunner cmd = new AssociatorRunner();
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
