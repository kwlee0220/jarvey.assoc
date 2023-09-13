package jarvey.assoc;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import utils.Indexed;
import utils.func.Funcs;
import utils.func.Tuple;
import utils.stream.FStream;

import jarvey.streams.model.Association.BinaryRelation;
import jarvey.streams.model.AssociationClosure;
import jarvey.streams.model.TrackletId;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AssociationCollection implements Iterable<AssociationClosure>  {
	private final String m_id;
	private final List<AssociationClosure> m_associations;
	
	public AssociationCollection(String id) {
		m_id = id;
		m_associations = Lists.newArrayList();
	}
	
	public String getId() {
		return m_id;
	}
	
	/**
	 * Collection에 포함된 association 객체의 갯수를 반환한다.
	 *
	 * @return	association 객체의 갯수.
	 */
	public long size() {
		return m_associations.size();
	}

	@Override
	public Iterator<AssociationClosure> iterator() {
		return m_associations.iterator();
	}
	
	public List<AssociationClosure> find(TrackletId trkId) {
		return Funcs.filter(m_associations, a -> a.containsTracklet(trkId));
	}
	
	public boolean exists(TrackletId trkId) {
		return Funcs.exists(m_associations, a -> a.containsTracklet(trkId));
	}

	/**
	 * 주어진 tracklet 들로 구성된 association 객체를 검색한다.
	 *
	 * @param key	검색 키로 사용할 tracklet 집합.
	 * @return	검색된 association 객체. 존재하지 않는 경우에는 null.
	 */
	public AssociationClosure get(Set<TrackletId> key) {
		return Funcs.findFirst(m_associations, a -> key.equals(a.getTracklets()));
	}

	/**
	 * 주어진 tracklet 들로 구성된 association 객체와 collection 내의 순번을 반환한다.
	 *
	 * @param key	검색 키로 사용할 tracklet 집합.
	 * @return	검색된 association 및 순번 객체. 존재하지 않는 경우에는 null.
	 */
	public Indexed<AssociationClosure> getIndexed(Set<TrackletId> key) {
		return FStream.from(m_associations)
						.zipWithIndex()
						.findFirst(t -> key.equals(t._1.getTracklets()))
						.map(t -> Indexed.with(t._1, t._2))
						.getOrNull();
	}
	
	/**
	 * 주어진 tracklet을 포함하는 모든 association을 반환한다.
	 *
	 * @param key	검색 키로 사용할 tracklet의 식별자.
	 * @return	검색된 association의 stream 객체.
	 */
	public FStream<AssociationClosure> findAll(TrackletId key) {
		return FStream.from(m_associations)
						.filter(a -> a.containsTracklet(key));
	}
	
	public FStream<Indexed<AssociationClosure>> findIndexedAll(TrackletId key) {
		return FStream.from(m_associations)
						.zipWithIndex()
						.map(t -> Indexed.with(t._1, t._2))
						.filter(idxed -> idxed.value().containsTracklet(key));
	}
	
	public AssociationClosure findSuperiorFirst(AssociationClosure key) {
		return Funcs.findFirst(m_associations, cl -> cl.isSuperior(key));
	}
	
	/**
	 * 주어진 tracklet들로 구성된 association을 collection에서 제거한다.
	 *
	 * @param key	tracklet id 집합
	 * @return	제거된 association 객체. 해당 키의 association 존재하지 않은 경우는 {@code null}.
	 */
	public AssociationClosure remove(Set<TrackletId> key) {
		return Funcs.removeFirstIf(m_associations, a -> a.match(key));
	}
	
	public AssociationClosure remove(int index) {
		return m_associations.remove(index);
	}
	
	public List<AssociationClosure> removeInferiors(AssociationClosure key) {
		return Funcs.removeIf(m_associations, cl -> cl.isInferior(key));
	}
	
	public List<AssociationClosure> add(AssociationClosure assoc) {
		return add(assoc, true);
	}
	
	public List<AssociationClosure> add(AssociationClosure assoc, boolean expandOnConflict) {
		// collection이 빈 경우는 바로 삽입하고 반환한다.
		if ( size() == 0 ) {
			m_associations.add(assoc);
			return Collections.singletonList(assoc);
		}
		
		List<AssociationClosure> updateds = Lists.newArrayList();
		Map<BinaryRelation,List<AssociationClosure>> groups = Maps.newHashMap();
		Iterator<AssociationClosure> iter = m_associations.iterator();
		while ( iter.hasNext() ) {
			AssociationClosure current = iter.next();

			BinaryRelation rel = current.relate(assoc);
			if ( rel == BinaryRelation.SAME ) {
				if ( current.getScore() >= assoc.getScore() ) {
					return Collections.emptyList();
				}
				else {
					iter.remove();
					m_associations.add(assoc);
					
					return Collections.singletonList(assoc);
				}
			}
			else {
				groups.computeIfAbsent(rel, k -> Lists.newArrayList()).add(current);
			}
		}
		
		List<AssociationClosure> superiors = groups.get(BinaryRelation.LEFT_SUBSUME);
		if ( superiors != null && superiors.size() > 0 ) {
			// 이미 더 superior한 association이 존재하는 경우
			return Collections.emptyList();
		}
		
		List<AssociationClosure> inferiors = groups.get(BinaryRelation.RIGHT_SUBSUME);
		if ( inferiors != null && inferiors.size() > 0 ) {
			// 기존 inferior한 association들을 모두 제거한다.
			Set<AssociationClosure> infSet = Sets.newHashSet(inferiors);
			Funcs.removeIf(m_associations, cl -> infSet.contains(cl));
		}
		
		boolean expanded = false;
		
		List<AssociationClosure> mergeables = groups.get(BinaryRelation.MERGEABLE);
		if ( mergeables != null && mergeables.size() > 0 ) {
			List<AssociationClosure> result = FStream.from(mergeables)
														.map(m -> m.merge(assoc))
														.flatMapIterable(m -> add(m, true))
														.toList();
			if ( result.size() > 0 ) {
				updateds.addAll(result);
				expanded = true;
			}
		}

		if ( groups.containsKey(BinaryRelation.CONFLICT) ) {
			if ( expandOnConflict ) {
				List<AssociationClosure> conflicts = groups.get(BinaryRelation.CONFLICT);
				for ( AssociationClosure conflict: conflicts ) {
					AssociationClosure merged = assoc.mergeWithoutConflicts(conflict, true);
					if ( merged != null ) {
						updateds.addAll(add(merged, false));
						expanded = true;
					}
				}
			}
		}
		if ( !expanded ) {
			m_associations.add(assoc);
			updateds.add(assoc);
		}
		
		return updateds;
	}
	
	public List<AssociationClosure> getBestAssociations() {
		return selectBestAssociations(m_associations);
	}
	
	public static List<AssociationClosure> selectBestAssociations(List<AssociationClosure> assocList) {
		List<AssociationClosure> bestAssocList = Lists.newArrayList();
		
		// collection에 속한 모든 association들을 길이와 score 값을 기준을 정렬시킨다.
		List<AssociationClosure> sorted = FStream.from(assocList)
												.sort(a -> Tuple.of(a.size(), a.getScore()), true)
												.toList();
		
		// 정렬된 association들을 차례대로 읽어 동일한 tracklet으로 구성된 inferior association들을
		// 삭제하는 방법으로 best association들을 구한다.
//		sorted = Lists.reverse(sorted);
		while ( sorted.size() > 0 ) {
			AssociationClosure best = sorted.remove(0);
			bestAssocList.add(best);
			Funcs.removeIf(sorted, best::intersectsTracklet);
		}
		
		return bestAssocList;
	}
	
	public void clear() {
		m_associations.clear();
	}
	
	@Override
	public String toString() {
		return m_associations.toString();
	}
}
