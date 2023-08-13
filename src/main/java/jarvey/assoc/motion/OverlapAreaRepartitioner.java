package jarvey.assoc.motion;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import jarvey.assoc.OverlapArea;
import jarvey.assoc.OverlapAreaRegistry;
import utils.func.Unchecked;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class OverlapAreaRepartitioner {
	private static final Logger s_logger = LoggerFactory.getLogger(OverlapAreaRepartitioner.class);
	
	private final String m_inputTopic;
	private final String m_outputTopic;
	private final KafkaConsumer<String,byte[]> m_consumer;
	private final KafkaProducer<String,byte[]> m_producer;
	private final OverlapAreaRegistry m_areaRegistry;
	private final Duration m_pollTimeout;
	private final Map<TopicPartition,OffsetAndMetadata> m_offsets = Maps.newHashMap();
	
	public OverlapAreaRepartitioner(KafkaConsumer<String,byte[]> consumer,
									KafkaProducer<String,byte[]> producer,
									String inputTopic, String outputTopic,
									OverlapAreaRegistry areaRegistry, Duration pollTimeout) {
		m_consumer = consumer;
		m_producer = producer;
		m_inputTopic = inputTopic;
		m_outputTopic = outputTopic;
		m_areaRegistry = areaRegistry;
		m_pollTimeout = pollTimeout;
	}
	
	private final ConsumerRebalanceListener m_rebalanceListener = new ConsumerRebalanceListener() {
		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			s_logger.info("provoking topic-partitions: {}", partitions);
			
			m_consumer.commitSync(m_offsets);
		}

		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			s_logger.info("assigning new topic-partitions: {}", partitions);
		}
	};
	
	public void run() {
		try {
			m_consumer.subscribe(Arrays.asList(m_inputTopic), m_rebalanceListener);
			
			while ( true ) {
				ConsumerRecords<String,byte[]> records = m_consumer.poll(m_pollTimeout);
				if ( records.count() > 0 ) {
					if ( s_logger.isDebugEnabled() ) {
						s_logger.debug("fetch {} records", records.count());
					}
					
					for ( TopicPartition tpart: records.partitions() ) {
						List<ConsumerRecord<String,byte[]>> partition = records.records(tpart);
						OffsetAndMetadata offsetMeta = repartition(partition);
						m_offsets.put(tpart, offsetMeta);
						
						if ( s_logger.isInfoEnabled() ) {
							s_logger.info("topic={}({}), nrecords={}, offset={}",
											tpart.topic(), tpart.partition(), partition.size(), offsetMeta.offset());
						}
					}
					m_consumer.commitAsync(m_offsets, null);
				}
			}
		}
		catch ( WakeupException ignored ) { }
		catch ( Exception e ) {
			s_logger.error("Unexpected error", e);
		}
		finally {
			Unchecked.runOrIgnore(() -> m_consumer.commitSync(m_offsets));
			Unchecked.runOrIgnore(m_consumer::close);
		}
	}
	
	private static final String NULL_ID = "_____";
	private OffsetAndMetadata repartition(List<ConsumerRecord<String, byte[]>> partition) {
		long lastOffset = -1;
		String lastNodeId = NULL_ID;
		String lastAreaId = null;
		for ( ConsumerRecord<String,byte[]> record: partition ) {
			String nodeId = record.key();
			
			String areaId;
			if ( nodeId.equals(lastNodeId) ) {
				areaId = lastAreaId;
			}
			else {
				areaId = lastAreaId = m_areaRegistry.findByNodeId(nodeId)
													.map(OverlapArea::getId)
													.getOrNull();
				lastNodeId = nodeId;
			}
			
			ProducerRecord<String,byte[]> outRecord = new ProducerRecord<>(m_outputTopic, areaId, record.value());
			m_producer.send(outRecord);
			lastOffset = record.offset();
		}
		
		return new OffsetAndMetadata(lastOffset + 1);
	}
}
