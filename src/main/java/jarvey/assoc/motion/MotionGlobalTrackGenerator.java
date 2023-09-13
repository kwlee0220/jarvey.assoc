package jarvey.assoc.motion;

import java.util.Collections;
import java.util.List;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.locationtech.jts.geom.Point;

import com.google.common.collect.Lists;

import utils.func.Funcs;
import utils.geo.util.GeoUtils;
import utils.stream.FStream;
import utils.stream.KeyedGroups;

import jarvey.assoc.AssociationCollection;
import jarvey.assoc.OverlapArea;
import jarvey.assoc.OverlapAreaRegistry;
import jarvey.streams.model.Association;
import jarvey.streams.model.AssociationClosure;
import jarvey.streams.model.GlobalTrack;
import jarvey.streams.model.GlobalTrack.State;
import jarvey.streams.model.LocalTrack;
import jarvey.streams.node.NodeTrack;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MotionGlobalTrackGenerator implements KeyValueMapper<String, NodeTrack,
																Iterable<KeyValue<String,GlobalTrack>>> {
	private static final long INTERVAL = 100;

	private final OverlapAreaRegistry m_areaRegistry;
	private final AssociationCollection m_collection;
	private final List<LocalTrack> m_trackBuffer = Lists.newArrayList();
	private long m_firstTs = -1;
	
	public MotionGlobalTrackGenerator(OverlapAreaRegistry areaRegistry, AssociationCollection associations) {
		m_areaRegistry = areaRegistry;
		m_collection = associations;
	}

	@Override
	public Iterable<KeyValue<String,GlobalTrack>> apply(String areaId, NodeTrack ntrack) {
		LocalTrack ltrack = LocalTrack.from(ntrack);

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
		
		// 현재까지의 association들 중에서 superior들만 추린다.
		List<AssociationClosure> bestAssocs = m_collection.getBestAssociations();
		
		// 'delete' track을 먼저 따로 뽑는다.
		List<LocalTrack> deleteds = FStream.from(m_trackBuffer)
											.filter(lt -> lt.isDeleted())
											.toList();

		// 버퍼에 수집된 local track들을 association에 따라 분류한다.
		// 만일 overlap area에 포함되지 않는 track의 경우에는 별도로 지정된 null로 분류한다.
		KeyedGroups<Association, LocalTrack> groups
				= FStream.from(m_trackBuffer)
						.filter(lt -> !lt.isDeleted())
						.groupByKey(lt -> findAssociation(lt, bestAssocs));
		m_trackBuffer.clear();
		m_trackBuffer.add(ltrack);
		m_firstTs = ltrack.getTimestamp();
		
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
	
	private Association findAssociation(LocalTrack ltrack, List<AssociationClosure> associations) {
		return Funcs.findFirst(associations, a -> a.containsTracklet(ltrack.getTrackletId()));
	}
	
	private String getOverlapAreaId(LocalTrack ltrack) {
		return Funcs.applyIfNotNull(m_areaRegistry.findByNodeId(ltrack.getNodeId()),
									OverlapArea::getId, null);
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
