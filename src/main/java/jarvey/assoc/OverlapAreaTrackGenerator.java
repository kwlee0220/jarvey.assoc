package jarvey.assoc;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import utils.func.FOption;
import utils.func.Funcs;
import utils.geo.util.GeoUtils;
import utils.stream.FStream;
import utils.stream.KeyedGroups;

import jarvey.streams.model.Association;
import jarvey.streams.model.GlobalTrack;
import jarvey.streams.model.GlobalTrack.State;
import jarvey.streams.model.JarveySerdes;
import jarvey.streams.model.LocalTrack;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.processor.KafkaConsumerRecordProcessor;
import jarvey.streams.serialization.json.GsonUtils;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class OverlapAreaTrackGenerator implements KafkaConsumerRecordProcessor<String,byte[]> {
	private static final Logger s_logger = LoggerFactory.getLogger(OverlapAreaTrackGenerator.class);

	private static final long INTERVAL = 100;
	
	private final OverlapAreaRegistry m_areaRegistry;
	private final AssociationStore m_assocStore;
	private final KafkaProducer<String,byte[]> m_producer;
	private final String m_outputTopic;
	
	private final Deserializer<NodeTrack> m_deser;
	private final Serializer<GlobalTrack> m_ser;
	private final AssociationCache m_cache = new AssociationCache(64);
	private final List<LocalTrack> m_trackBuffer = Lists.newArrayList();
	private long m_firstTs = -1;
	
	public OverlapAreaTrackGenerator(OverlapAreaRegistry areaRegistry,
										AssociationStore assocStore,
										KafkaProducer<String,byte[]> producer, String outputTopic) {
		m_areaRegistry = areaRegistry;
		m_assocStore = assocStore;
		m_producer = producer;
		m_outputTopic = outputTopic;
		
		m_deser = JarveySerdes.NodeTrack().deserializer();
		m_ser = GsonUtils.getSerde(GlobalTrack.class).serializer();
	}

	@Override
	public void close() throws Exception {
		while ( m_trackBuffer.size() > 0 ) {
			toGlobalTrackBatch(m_trackBuffer).forEach(this::publish);
			m_trackBuffer.clear();
		}
	}

	@Override
	public FOption<OffsetAndMetadata> process(ConsumerRecord<String, byte[]> record) {
		NodeTrack track = m_deser.deserialize(record.topic(), record.value());
		
		OverlapArea area = m_areaRegistry.findByNodeId(track.getNodeId());
		List<KeyValue<String,GlobalTrack>> kvalues = (area != null)
													? toOverlapAreaGlobalTracks(area, track)
													: toNonOverlapAreaGlobalTracks(track);
		kvalues.forEach(this::publish);
		
		return FOption.of(new OffsetAndMetadata(record.offset() + 1));
	}

	@Override
	public void timeElapsed(long expectedTs) {
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("time-elapsed: expected-ts={}", expectedTs);
		}
		
		if ( m_trackBuffer.size() > 0 && (expectedTs - m_firstTs) >= INTERVAL ) {
			toGlobalTrackBatch(m_trackBuffer).forEach(this::publish);
			m_trackBuffer.clear();
		}
	}

	@Override
	public long extractTimestamp(ConsumerRecord<String, byte[]> record) {
		return m_deser.deserialize(record.topic(), record.value()).getTimestamp();
	}
	
	private void publish(KeyValue<String,GlobalTrack> keyedGlobalTrack) {
		byte[] bytes = m_ser.serialize(m_outputTopic, keyedGlobalTrack.value);
		m_producer.send(new ProducerRecord<>(m_outputTopic, keyedGlobalTrack.key, bytes));
	}
	
	private List<KeyValue<String,GlobalTrack>> toNonOverlapAreaGlobalTracks(NodeTrack track) {
		LocalTrack ltrack = LocalTrack.from(track);
		
		// 현재까지의 association들 중에서 superior들만 추린다.
		Association assoc = findAssociation(ltrack);

		GlobalTrack gtrack = (assoc != null)
								? GlobalTrack.from(assoc, ltrack, null)
								: GlobalTrack.from(ltrack, null);
		return Collections.singletonList(KeyValue.pair(ltrack.getNodeId(), gtrack));	
	}
	
	private List<KeyValue<String,GlobalTrack>> toOverlapAreaGlobalTracks(OverlapArea area, NodeTrack track) {
		// 카메라로부터 일정 거리 이내의 track 정보만 활용한다.
		// 키 값도 node-id에서 overlap-area id로 변경시킨다.
		if ( !withAreaDistance(area, track) ) {
			return Collections.emptyList();
		}
		
		LocalTrack ltrack = LocalTrack.from(track);
		
		long ts = ltrack.getTimestamp();
		if ( m_firstTs < 0 ) {
			m_firstTs = ltrack.getTimestamp();
		}

		// 일정기간 동안 track을 모아서 한번에 처리하도록 한다. (예: 100ms)
		// 지정된 기간이 넘으면 모인 track에 대해 combine을 수행한다.
		// 만일 기간을 경과하지 않으면 바로 반환한다.
		if ( (ts - m_firstTs) < INTERVAL ) {
			m_trackBuffer.add(ltrack);
			return Collections.emptyList();
		}
		
		List<KeyValue<String,GlobalTrack>> keyedGTracks = toGlobalTrackBatch(m_trackBuffer);
		m_trackBuffer.clear();
		m_trackBuffer.add(ltrack);
		m_firstTs = ltrack.getTimestamp();
		
		return keyedGTracks;
	}
	
	private List<KeyValue<String,GlobalTrack>> toGlobalTrackBatch(List<LocalTrack> ltracks) {
		// 'delete' track을 먼저 따로 뽑는다.
		List<LocalTrack> deleteds = FStream.from(ltracks)
											.filter(lt -> lt.isDeleted())
											.toList();

		// 버퍼에 수집된 local track들을 association에 따라 분류한다.
		// 만일 overlap area에 포함되지 않는 track의 경우에는 별도로 지정된
		// null로 분류한다.
		KeyedGroups<Association, LocalTrack> groups
				= FStream.from(m_trackBuffer)
						.filter(lt -> !lt.isDeleted())
						.groupByKey(lt -> findAssociation(lt));
		
		// association이 존재하는 경우는 동일 assoication끼리 묶어 평균 값을 사용한다.
		List<GlobalTrack> gtracks = groups.stream()
											.filter(kv -> kv.key() != null)
											.map(kv -> average(kv.key(), kv.value()))
											.toList();
		
		// Association이 없는 track들은 각 trackletId별로 하나의 global track을 생성한다.
		List<LocalTrack> unassociateds = groups.remove(null)
												.getOrElse(Collections.emptyList());
		FStream.from(unassociateds)
				.groupByKey(lt -> lt.getTrackletId())
				.stream()
				.map(kv -> average(kv.value()))
				.forEach(gtracks::add);
		
		// 생성된 global track들을 timestamp를 기준으로 정렬시킨다.
		gtracks = FStream.from(gtracks)
						.sort(GlobalTrack::getTimestamp)
						.toList();
		FStream.from(deleteds)
				.map(lt -> GlobalTrack.from(lt, getOverlapAreaId(lt)))
				.forEach(gtracks::add);
		
		List<KeyValue<String,GlobalTrack>> result = Funcs.map(gtracks, gt -> KeyValue.pair(gt.getKey(), gt));
		return result;
	}
	
	private boolean withAreaDistance(OverlapArea area, NodeTrack track) {
		if ( track.isDeleted() ) {
			return true;
		}
		
		double threshold = area.getDistanceThreshold(track.getNodeId());
		return track.getDistance() <= threshold;
	}
	
	private String getOverlapAreaId(LocalTrack ltrack) {
		return Funcs.applyIfNotNull(m_areaRegistry.findByNodeId(ltrack.getNodeId()),
									OverlapArea::getId, null);
	}
	
	private Association findAssociation(LocalTrack ltrack) {
		return m_cache.get(ltrack.getTrackletId())
						.getOrElse(() -> {
							try {
								Association assoc = m_assocStore.getAssociation(ltrack.getTrackletId());
								m_cache.put(ltrack.getTrackletId(), assoc);
								return null;
							}
							catch ( SQLException e ) {
								return null;
							}
						});
	}
	
	private GlobalTrack average(Association assoc, List<LocalTrack> ltracks) {
		OverlapArea area = m_areaRegistry.findByNodeId(ltracks.get(0).getNodeId());
		String areaId = area != null ? area.getId() : null;
		return GlobalTrack.from(assoc, ltracks, areaId);
	}
	
	private GlobalTrack average(List<LocalTrack> ltracks) {
		LocalTrack repr = Funcs.max(ltracks, LocalTrack::getTimestamp);
		String id = repr.getKey();
		Point avgLoc = GeoUtils.average(Funcs.map(ltracks, LocalTrack::getLocation));
	
		OverlapArea area = m_areaRegistry.findByNodeId(repr.getNodeId());
		String areaId = area != null ? area.getId() : null;
		
		return new GlobalTrack(id, State.ISOLATED, areaId, avgLoc, null,
								repr.getFirstTimestamp(), repr.getTimestamp());
	}
}
