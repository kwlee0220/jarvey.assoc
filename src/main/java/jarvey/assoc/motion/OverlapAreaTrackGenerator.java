package jarvey.assoc.motion;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MinMaxPriorityQueue;

import utils.func.FOption;
import utils.func.Funcs;
import utils.func.KeyValue;
import utils.geo.util.GeoUtils;
import utils.stream.FStream;

import jarvey.assoc.AssociationClosure;
import jarvey.assoc.AssociationClosure.DAO;
import jarvey.assoc.OverlapArea;
import jarvey.assoc.OverlapAreaRegistry;
import jarvey.streams.EventCollectingWindowAggregation;
import jarvey.streams.HoppingWindowManager;
import jarvey.streams.model.GlobalTrack;
import jarvey.streams.model.LocalTrack;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.processor.KafkaConsumerRecordProcessor;
import jarvey.streams.serialization.json.GsonUtils;
import jarvey.streams.updatelog.KeyedUpdateLogs;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class OverlapAreaTrackGenerator implements KafkaConsumerRecordProcessor<String,byte[]> {
	private static final Logger s_logger = LoggerFactory.getLogger(OverlapAreaTrackGenerator.class);

	private static final int MAX_SIZE = 2048;
	private static final long LAG_MILLIS = Duration.ofSeconds(20).toMillis();
	
	private final OverlapAreaRegistry m_areaRegistry;
	private final KeyedUpdateLogs<NodeTrack> m_trackUpdateLogs;
	private final KafkaProducer<String,byte[]> m_producer;
	private final String m_outputTopic;
	
	private final Deserializer<AssociationClosure.DAO> m_assocDeserializer;
	private final Serializer<GlobalTrack> m_serializer;
	private final MinMaxPriorityQueue<GlobalTrack> m_heap;
	
	public OverlapAreaTrackGenerator(OverlapAreaRegistry areaRegistry,
										KeyedUpdateLogs<NodeTrack> trackUpdateLogs,
										KafkaProducer<String,byte[]> producer, String outputTopic) {
		m_areaRegistry = areaRegistry;
		m_trackUpdateLogs = trackUpdateLogs;
		m_producer = producer;
		m_outputTopic = outputTopic;
		
		m_assocDeserializer = GsonUtils.getSerde(AssociationClosure.DAO.class).deserializer();
		m_serializer = GsonUtils.getSerde(GlobalTrack.class).serializer();
		
		m_heap = MinMaxPriorityQueue
						.orderedBy(Comparator.comparing(GlobalTrack::getTimestamp))
						.maximumSize(MAX_SIZE)
						.create();
	}

	@Override
	public void close() throws Exception {
		while ( m_heap.size() > 0 ) {
			publish(m_heap.poll());
		}
	}

	@Override
	public FOption<OffsetAndMetadata> process(ConsumerRecord<String, byte[]> record) {
		DAO assoc = m_assocDeserializer.deserialize(record.topic(), record.value());
		
		List<String> trackKeys = Funcs.map(assoc.getTrackletIds(), TrackletId::toString);
		TrackletId outputTrkId = new TrackletId(record.key(), ""+assoc.getTimestamp());

		HoppingWindowManager winMgr = HoppingWindowManager.ofWindowSize(Duration.ofMillis(100));
		EventCollectingWindowAggregation<NodeTrack> aggr = new EventCollectingWindowAggregation<>(winMgr);

		// association에 참여하는 NodeTrack들을 얻어 GlobalTrack 객체를 생성한다.
		List<GlobalTrack> gtracks
			= m_trackUpdateLogs.streamOfKeys(trackKeys)
								// 검출 카메라로부터 일정 거리 밖에 있는 track들은 제외시킨다.
								.filter(kv -> isWithinDistance(kv.key(), kv.value()))
								.map(KeyValue::value)
								.flatMapIterable(aggr::collect)
								.map(w -> merge(outputTrkId, w.value()))
								.toList();
		gtracks.forEach(m_heap::add);
		publishUpto(assoc.getTimestamp() - LAG_MILLIS);
		
		return FOption.of(new OffsetAndMetadata(record.offset() + 1));
	}

	@Override
	public void timeElapsed(long expectedTs) {
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("time-elapsed: expected-ts={}", expectedTs);
		}

		publishUpto(expectedTs - LAG_MILLIS);
	}

	@Override
	public long extractTimestamp(ConsumerRecord<String, byte[]> record) {
		return m_assocDeserializer.deserialize(record.topic(), record.value()).getTimestamp();
	}
	
	private void publishUpto(long upperTs) {
		while ( m_heap.size() > 0 ) {
			if ( m_heap.peekFirst().getTimestamp() >= upperTs ) {
				break;
			}
			publish(m_heap.pollFirst());
		}
	}
	
	private void publish(GlobalTrack gtrack) {
		byte[] bytes = m_serializer.serialize(m_outputTopic, gtrack);
		m_producer.send(new ProducerRecord<>(m_outputTopic, gtrack.getNodeId(), bytes));
	}
	
	private GlobalTrack merge(TrackletId outputTrkId, List<NodeTrack> tracks) {
		List<Point> pts = FStream.from(tracks)
								.filterNot(NodeTrack::isDeleted)
								.map(NodeTrack::getLocation)
								.toList();
		if ( pts.isEmpty() ) {
			return GlobalTrack.deleted(LocalTrack.from(tracks.get(0)), outputTrkId.getNodeId());
		}
		Point loc = GeoUtils.average(pts);
		List<LocalTrack> ltracks = FStream.from(tracks)
											.filterNot(NodeTrack::isDeleted)
											.map(LocalTrack::from)
											.toList();
		long ts = FStream.from(tracks).map(NodeTrack::getTimestamp).max();
		
		return new GlobalTrack(outputTrkId, outputTrkId.getNodeId(), loc, ltracks, ts);
	}
	
	private boolean isWithinDistance(String areaId, NodeTrack track) {
		OverlapArea area = m_areaRegistry.get(areaId);
		if ( area == null ) {
			return false;
		}
		
		double threshold = area.getDistanceThreshold(track.getNodeId());
		return track.isDeleted() || track.getDistance() <= threshold;
	}
}
