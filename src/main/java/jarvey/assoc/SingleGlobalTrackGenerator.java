package jarvey.assoc;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import utils.func.FOption;

import jarvey.streams.model.GlobalTrack;
import jarvey.streams.model.LocalTrack;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.processor.KafkaConsumerRecordProcessor;
import jarvey.streams.serialization.json.GsonUtils;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class SingleGlobalTrackGenerator implements KafkaConsumerRecordProcessor<String,byte[]> {
	private final OverlapAreaRegistry m_areaRegistry;
	private final KafkaProducer<String,byte[]> m_producer;
	private final String m_outputTopic;
	
	private final Deserializer<NodeTrack> m_deserializer;
	private final Serializer<GlobalTrack> m_serializer;
	
	public SingleGlobalTrackGenerator(OverlapAreaRegistry areaRegistry,
										KafkaProducer<String,byte[]> producer, String outputTopic) {
		m_areaRegistry = areaRegistry;
		m_producer = producer;
		m_outputTopic = outputTopic;
		
		m_deserializer = GsonUtils.getSerde(NodeTrack.class).deserializer();
		m_serializer = GsonUtils.getSerde(GlobalTrack.class).serializer();
	}

	@Override
	public void close() throws Exception {
		m_producer.close();
	}

	@Override
	public void timeElapsed(long expectedTs) {
	}

	@Override
	public long extractTimestamp(ConsumerRecord<String, byte[]> record) {
		return m_deserializer.deserialize(record.topic(), record.value()).getTimestamp();
	}

	@Override
	public FOption<OffsetAndMetadata> process(ConsumerRecord<String, byte[]> record) {
		NodeTrack track = m_deserializer.deserialize(record.topic(), record.value());
		
		OverlapArea area = m_areaRegistry.findByNodeId(track.getNodeId()).getOrNull();
		if ( area == null ) {
			GlobalTrack gtrack = GlobalTrack.from(LocalTrack.from(track), null);
			
			byte[] bytes = m_serializer.serialize(record.key(), gtrack);
			m_producer.send(new ProducerRecord<>(m_outputTopic, record.key(), bytes));
		}
		
		return FOption.of(new OffsetAndMetadata(record.offset()));
	}
}
