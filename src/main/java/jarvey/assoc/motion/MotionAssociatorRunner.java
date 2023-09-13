package jarvey.assoc.motion;


import java.util.Properties;
import java.util.Set;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import utils.UsageHelp;
import utils.jdbc.JdbcParameters;
import utils.jdbc.JdbcProcessor;

import jarvey.assoc.AssociationCollection;
import jarvey.streams.KafkaAdmins;
import jarvey.streams.KafkaParameters;
import jarvey.streams.TrackTimestampExtractor;
import jarvey.streams.model.JarveySerdes;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrack;

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
public final class MotionAssociatorRunner implements Runnable {
	private static final Logger s_logger = LoggerFactory.getLogger(MotionAssociatorRunner.class);

	private static final String APPLICATION_ID = "motion-associator";
	private static final TrackTimestampExtractor TS_EXTRACTOR = new TrackTimestampExtractor();
	
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	@Mixin private KafkaParameters m_kafkaParams;
	@Mixin private JdbcParameters m_jdbcParams;
	@Mixin private MotionAssociationParams m_assocParams;
	
	@Override
	public void run() {
		try {
			if ( m_kafkaParams.getApplicationId() == null ) {
				m_kafkaParams.setApplicationId(APPLICATION_ID);
			}
			
			JdbcProcessor jdbc = m_jdbcParams.createJdbcProcessor();
			AssociationCollection associations = new AssociationCollection("motion-associations");
			Set<TrackletId> closedTracklets = Sets.newHashSet();
			AssociationCollection finalAssociations = new AssociationCollection("final-motion-associations");
			
			MotionAssociationStreamBuilder topoBuilder
				= new MotionAssociationStreamBuilder(m_assocParams, jdbc, associations, closedTracklets,
														finalAssociations);
			
			StreamsBuilder builder = new StreamsBuilder();
			KStream<String,NodeTrack> nodeTracks =
				builder
					.stream(m_assocParams.getNodeTracksTopic(),
							Consumed.with(Serdes.String(), JarveySerdes.NodeTrack())
									.withName("from-node-tracks")
									.withTimestampExtractor(TS_EXTRACTOR)
									.withOffsetResetPolicy(m_kafkaParams.getAutoOffsetReset()));
			topoBuilder.build(nodeTracks);
			
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
		MotionAssociatorRunner runner = new MotionAssociatorRunner();
		CommandLine commandLine = new CommandLine(runner).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				runner.run();
			}
		}
		catch ( Throwable e ) {
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
}
