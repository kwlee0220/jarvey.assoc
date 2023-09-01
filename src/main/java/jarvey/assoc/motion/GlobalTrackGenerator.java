package jarvey.assoc.motion;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.streams.kstream.ValueMapperWithKey;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import utils.func.Funcs;
import utils.func.Tuple;
import utils.stream.FStream;
import utils.stream.KeyedGroups;

import jarvey.assoc.AssociationCollection;
import jarvey.assoc.motion.MotionBasedAssociationContext.Session;
import jarvey.streams.model.AssociationClosure;
import jarvey.streams.model.GlobalTrack;
import jarvey.streams.model.LocalTrack;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrack;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GlobalTrackGenerator implements ValueMapperWithKey<String, NodeTrack, Iterable<GlobalTrack>> {
	private static final long INTERVAL = 100;

	private final MotionBasedAssociationContext m_context;
	private Map<String, List<LocalTrack>> m_lastLocs = Maps.newHashMap();
	
	public GlobalTrackGenerator(MotionBasedAssociationContext context) {
		m_context = context;
	}

	@Override
	public Iterable<GlobalTrack> apply(String areaId, NodeTrack ntrack) {
		LocalTrack ltrack = LocalTrack.from(ntrack);
		if ( areaId == null ) {
			// overlap area 이외의 장소에 설치된 카메라에서 추적된 이벤트인 경우에는
			// 바로 global track을 생성한다.
			GlobalTrack gtrack = ntrack.isDeleted()
										? GlobalTrack.deleted(ltrack, null)
										: GlobalTrack.from(ltrack, null);
			return Collections.singleton(gtrack);
		}
		
		Session session = m_context.get(areaId);
		if ( session == null ) {
			return Collections.emptyList();
		}
		
		AssociationCollection<AssociationClosure> collection = session.m_collection;
		if ( collection == null ) {
			// 'areaId'에 해당하는 지역에 association이 전혀 없는 경우
			GlobalTrack gtrack = GlobalTrack.from(ltrack, areaId);
			return Collections.singleton(gtrack);
		}
		
		// 일정기간 동안 track을 모아서 한번에 처리하도록 한다. (예: 100ms)
		List<LocalTrack> bucket = m_lastLocs.computeIfAbsent(areaId, k -> Lists.newArrayList());
		bucket.add(ltrack);
		
		// 지정된 기간이 넘으면 모인 track에 대해 combine을 수행한다.
		// 만일 기간을 경과하지 않으면 바로 반환한다.
		long ts = ltrack.getTimestamp();
		List<LocalTrack> expiredTracks = Funcs.filter(bucket, lt -> (ts - lt.getTimestamp()) >= INTERVAL);
		if ( expiredTracks.isEmpty() ) {
			return Collections.emptyList();
		}
		
		List<AssociationClosure> bestAssocs = collection.getBestAssociations();

		// 각 track에 대해 associate된 다른 track들과 combine시킨다
		// Combine된 global track들은 timestamp 순서대로 정렬시킨다.
		List<GlobalTrack> gtracks
			= FStream.from(expiredTracks)
					.map(lt -> combine(areaId, bestAssocs, lt, expiredTracks))
					.distinct(GlobalTrack::getKey)
					// deleted 이벤트를 가장 마지막으로 몰기 위해 delete 여부를 고려한 정렬을 시도.
					.sort(gt -> Tuple.of(gt.getTimestamp(), gt.isDeleted() ? 1 : 0))
					.toList();
		
		// pending list에서 expire된 track들을 제거한다.
		bucket.removeAll(expiredTracks);
		if ( bucket.isEmpty() ) {
			m_lastLocs.remove(areaId);
		}
		
		return gtracks;
	}
	
	private GlobalTrack combine(String areaId, List<AssociationClosure> collection,
								LocalTrack ltrack, List<LocalTrack> candidates) {
		// ltrack과 관련된 association들 중에서 가장 높은 score를 association을 찾는다. 
		// 만일 관련 association이 없는 경우에는 단일 track의 global track을 생성한다.
		//
		TrackletId trkId = ltrack.getTrackletId();
		AssociationClosure assoc = Funcs.findFirst(collection, a -> a.containsTracklet(trkId));
		if ( assoc == null ) {
			return GlobalTrack.from(assoc, ltrack, areaId);
		}
		
		if ( ltrack.isDeleted() ) {
			return GlobalTrack.deleted(assoc, ltrack, areaId);
		}
		
		// 검색된 best association을 기준으로 연관된 node들의 track들 중에서
		// 가장 근접한 track들을 뽑는다.
		KeyedGroups<TrackletId,LocalTrack> groups
			= FStream.from(candidates)
					.filter(lt -> assoc.getTracklets().contains(lt.getTrackletId()))
					.groupByKey(LocalTrack::getTrackletId);
		List<LocalTrack> supports
			 = groups.stream()
					.filterKey(tid -> !tid.equals(ltrack.getTrackletId()))
					.toValueStream()
					.flatMapNullable(srchRange -> findClosest(ltrack.getTimestamp(), srchRange))
					.toList();
		supports.add(ltrack);
		
		return GlobalTrack.from(assoc, supports, areaId);
	}
	
	private static LocalTrack findClosest(long ts, List<LocalTrack> range) {
		return FStream.from(range)
						.filterNot(LocalTrack::isDeleted)
						.min(lt -> Math.abs(lt.getTimestamp()-ts));
	}
}
