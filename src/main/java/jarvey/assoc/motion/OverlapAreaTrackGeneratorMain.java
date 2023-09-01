package jarvey.assoc.motion;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import com.google.common.collect.Lists;

import utils.UsageHelp;
import utils.func.CheckedRunnable;
import utils.jdbc.JdbcParameters;
import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;

import jarvey.assoc.OverlapAreaRegistry;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.processor.AbstractKafkaTopicProcessorDriver;
import jarvey.streams.processor.KafkaTopicPartitionProcessor;
import jarvey.streams.serialization.json.GsonUtils;
import jarvey.streams.updatelog.KeyedUpdateLogs;

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
@Command(name="overlap-area-track-generator",
			parameterListHeading = "Parameters:%n",
			optionListHeading = "Options:%n",
			description="Generate global tracks in an overlap area.")
public class OverlapAreaTrackGeneratorMain extends AbstractKafkaTopicProcessorDriver<String,byte[]>
											implements CheckedRunnable {
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	
	@Mixin private JdbcParameters m_jdbcParams;
	private String m_indexTableName = "node_tracks_repartition_index";
	
	private List<String> m_inputTopics = Lists.newArrayList("motion-associations");
	private String m_nodeTrackTopic = "node-tracks-repartition";
	private String m_outputTopic = "global-tracks-overlap";
	
	private String m_overlapAreaFilePath = "overlap_areas.yaml";
	
	private OverlapAreaRegistry m_areaRegistry = null;

	@Option(names={"--overlap-area"}, paramLabel="overlap-area-descriptor",
			description="overlap area description file path.")
	public void setOverlapAreaFile(String path) {
		m_overlapAreaFilePath = path;
	}
	
	@Override
	protected Collection<String> getInputTopics() {
		return m_inputTopics;
	}
	@Option(names={"--input"}, paramLabel="topic names", description="input topic names")
	public void setInputTopics(String names) {
		m_inputTopics = FStream.of(names.split(",")).map(String::trim).toList();
	}
	
	protected String getNodeTrackTopic() {
		return m_nodeTrackTopic;
	}
	@Option(names={"--track-topic"}, paramLabel="topic name", description="node-track topic name")
	public void setNodeTrackTopic(String name) {
		m_nodeTrackTopic = name;
	}
	
	@Option(names={"--index-table"}, paramLabel="table name", description="node-track index table name")
	public void setIndexTableName(String name) {
		m_indexTableName = name.replace('-', '_');
	}
	
	@Option(names={"--output"}, paramLabel="topic-name", description="output topic name")
	public void setOutputTopic(String name) {
		m_outputTopic = name;
	}

	@Override
	protected KafkaTopicPartitionProcessor<String,byte[]> allocateProcessor(TopicPartition tpart)
		throws IOException {
		if ( m_areaRegistry == null ) {
			m_areaRegistry = OverlapAreaRegistry.load(new File(m_overlapAreaFilePath));
		}

		Properties consumerProps = m_kafkaParams.toConsumerProperties();
		JdbcProcessor jdbc = m_jdbcParams.createJdbcProcessor();
		Deserializer<NodeTrack> trackDeser = GsonUtils.getSerde(NodeTrack.class).deserializer();
		KeyedUpdateLogs<NodeTrack> trackUpdateLogs
			= new KeyedUpdateLogs<>(jdbc, m_indexTableName, consumerProps, m_nodeTrackTopic, trackDeser);
		
		Properties producerProps = m_kafkaParams.toProducerProperties();
		KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);

		OverlapAreaTrackGenerator gen
			= new OverlapAreaTrackGenerator(m_areaRegistry, trackUpdateLogs, producer, m_outputTopic);
		return KafkaTopicPartitionProcessor.wrap(gen);
	}
	
	@SuppressWarnings("deprecation")
	public static final void main(String... args) throws Exception {
		OverlapAreaTrackGeneratorMain cmd = new OverlapAreaTrackGeneratorMain();
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
