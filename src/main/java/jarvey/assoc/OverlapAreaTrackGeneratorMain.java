package jarvey.assoc;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;

import com.google.common.collect.Lists;

import utils.UsageHelp;
import utils.func.CheckedRunnable;
import utils.jdbc.JdbcParameters;
import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;

import jarvey.streams.processor.AbstractKafkaTopicProcessorDriver;
import jarvey.streams.processor.KafkaTopicPartitionProcessor;

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

	private List<String> m_inputTopics = Lists.newArrayList("node-tracks");
	private String m_outputTopic = "global-tracks";
	
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
		
		JdbcProcessor jdbc = m_jdbcParams.createJdbcProcessor();
		AssociationStore assocStore = new AssociationStore(jdbc);
		
		Properties producerProps = m_kafkaParams.toProducerProperties();
		KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);

		OverlapAreaTrackGenerator gen
			= new OverlapAreaTrackGenerator(m_areaRegistry, assocStore, producer, m_outputTopic);
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
