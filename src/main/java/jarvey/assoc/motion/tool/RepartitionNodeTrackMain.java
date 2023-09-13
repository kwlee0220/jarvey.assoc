package jarvey.assoc.motion.tool;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.UsageHelp;
import utils.func.CheckedRunnable;
import utils.func.FOption;

import jarvey.assoc.OverlapArea;
import jarvey.assoc.OverlapAreaRegistry;
import jarvey.streams.KafkaAdmins;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.processor.AbstractKafkaTopicProcessorDriver;
import jarvey.streams.processor.KafkaTopicPartitionProcessor;
import jarvey.streams.serialization.json.GsonUtils;

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
@Command(name="node-track-repartitioner",
			parameterListHeading = "Parameters:%n",
			optionListHeading = "Options:%n",
			description="Repartition the node-tracks based on their overlap-areas.")
public class RepartitionNodeTrackMain extends AbstractKafkaTopicProcessorDriver<String,byte[]>
										implements CheckedRunnable {
	private static final Logger s_logger = LoggerFactory.getLogger(RepartitionNodeTrackMain.class);
	
	private static final String APP_ID = "node-track-repartitioner";
	private static final String DEFAULT_TOPIC_INPUT = "node-tracks";
	private static final String DEFAULT_TOPIC_OUTPUT = DEFAULT_TOPIC_INPUT + "-repartition";
	
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;

	@Option(names={"--overlap-area"}, paramLabel="overlap-area-descriptor",
			description="overlap area description file path.")
	private String m_overlapAreaFilePath = "overlap_areas.yaml";

	@Option(names={"--input"}, paramLabel="topic-name", description="input topic name")
	private String m_inputTopic = DEFAULT_TOPIC_INPUT;

	@Option(names={"--output"}, paramLabel="topic-name", description="output global-track topic.")
	private String m_outputTopic = DEFAULT_TOPIC_OUTPUT;

	@Override
	protected Collection<String> getInputTopics() {
		return Arrays.asList(m_inputTopic);
	}

	@Override
	protected KafkaTopicPartitionProcessor<String,byte[]> allocateProcessor(TopicPartition tpart)
		throws IOException {
		OverlapAreaRegistry registry = OverlapAreaRegistry.load(new File(m_overlapAreaFilePath));

		Properties producerProps = m_kafkaParams.toProducerProperties();
		KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);
		return new Repartitioner(registry, producer, m_outputTopic);
	}

	@Override
	public void run() throws Throwable {
		if ( m_kafkaParams.getApplicationId() == null ) {
			m_kafkaParams.setApplicationId(APP_ID);
		}
		
		KafkaAdmins admin = new KafkaAdmins(m_kafkaParams.getBootstrapServers());
		try {
			if ( !admin.existsTopic(m_inputTopic) ) {
				s_logger.warn("input topic('{}') is not present. create one", m_inputTopic);
				admin.createTopic(m_inputTopic, 1, 1);
			}
		}
		catch ( Exception e ) {
			s_logger.warn("fails to get topic description", e);
		}
		
		super.run();
	}
	
	@SuppressWarnings("deprecation")
	public static final void main(String... args) throws Exception {
		RepartitionNodeTrackMain cmd = new RepartitionNodeTrackMain();
		
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
	
	private static class Repartitioner implements KafkaTopicPartitionProcessor<String,byte[]> {
		private final String m_outputTopic;
		private final KafkaProducer<String,byte[]> m_producer;
		private final Deserializer<NodeTrack> m_deserializer; 
		private final OverlapAreaRegistry m_areaRegistry;
		
		public Repartitioner(OverlapAreaRegistry areaRegistry,
							KafkaProducer<String,byte[]> producer, String outputTopic) {
			m_areaRegistry = areaRegistry;
			m_producer = producer;
			m_outputTopic = outputTopic;
			
			Serde<NodeTrack> serde = GsonUtils.getSerde(NodeTrack.class);
			m_deserializer = serde.deserializer();
		}

		@Override
		public void close() throws Exception { }

		@Override
		public FOption<OffsetAndMetadata> process(TopicPartition tpart,
												List<ConsumerRecord<String, byte[]>> partition) throws Exception {
			long lastOffset = -1;
			String lastNodeId = "__________";
			String lastAreaId = null;
			
			for ( ConsumerRecord<String,byte[]> record: partition ) {
				String nodeId = record.key();
				
				String areaId;
				if ( nodeId.equals(lastNodeId) ) {
					areaId = lastAreaId;
				}
				else {
					OverlapArea area = m_areaRegistry.findByNodeId(nodeId);
					areaId = lastAreaId = (area != null) ? area.getId() : null;
					lastNodeId = nodeId;
				}
				
				ProducerRecord<String,byte[]> outRecord = new ProducerRecord<>(m_outputTopic, areaId, record.value());
				m_producer.send(outRecord);
				lastOffset = record.offset();
			}
			
			return (lastOffset) >= 0 ? FOption.of(new OffsetAndMetadata(lastOffset+1)) : FOption.empty();
		}

		@Override
		public long extractTimestamp(ConsumerRecord<String, byte[]> record) {
			return m_deserializer.deserialize(record.topic(), record.value()).getTimestamp();
		}
	}
}
