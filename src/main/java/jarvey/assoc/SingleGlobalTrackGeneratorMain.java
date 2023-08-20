package jarvey.assoc;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;

import utils.UsageHelp;
import utils.func.CheckedRunnable;

import jarvey.streams.KafkaParameters;
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
@Command(name="single-global-track-generator",
			parameterListHeading = "Parameters:%n",
			optionListHeading = "Options:%n",
			description="Generate global tracks from node tracks in non-overlapping area")
public class SingleGlobalTrackGeneratorMain extends AbstractKafkaTopicProcessorDriver<String,byte[]>
											implements CheckedRunnable {
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	@Mixin private KafkaParameters m_kafkaParams;

	@Option(names={"--overlap-area"}, paramLabel="overlap-area-descriptor",
			description="overlap area description file path.")
	private String m_overlapAreaFilePath = "overlap_areas.yaml";

	@Option(names={"--input"}, paramLabel="topic-name", description="input topic name")
	private String m_inputTopic = "node-tracks";

	@Option(names={"--output"}, paramLabel="topic-name",
					description="output global-track topic. (default: global-tracks-single")
	private String m_outputTopic = "global-tracks-single";
	
	private OverlapAreaRegistry m_areaRegistry = null;
	private KafkaTopicPartitionProcessor<String, byte[]> m_trackGenerator;

	@Override
	public void close() throws Exception {
		if ( m_trackGenerator != null ) {
			m_trackGenerator.close();
		}
	}

	@Override
	protected KafkaConsumer<String, byte[]> openKafkaConsumer() {
		return new KafkaConsumer<>(m_kafkaParams.toConsumerProperties());
	}

	@Override
	protected Collection<String> getInputTopics() {
		return Arrays.asList(m_inputTopic);
	}

	@Override
	protected KafkaTopicPartitionProcessor<String, byte[]> allocateProcessor(TopicPartition tpart)
		throws IOException {
		if ( m_areaRegistry == null ) {
			m_areaRegistry = OverlapAreaRegistry.load(new File(m_overlapAreaFilePath));
		}
		
		Properties producerProps = m_kafkaParams.toProducerProperties();
		KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);
		SingleGlobalTrackGenerator gen = new SingleGlobalTrackGenerator(m_areaRegistry, producer,
																		m_outputTopic);
		return KafkaTopicPartitionProcessor.wrap(gen);
	}
	
	@SuppressWarnings("deprecation")
	public static final void main(String... args) throws Exception {
		SingleGlobalTrackGeneratorMain cmd = new SingleGlobalTrackGeneratorMain();
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
