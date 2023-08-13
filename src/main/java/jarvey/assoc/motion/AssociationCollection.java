package jarvey.assoc.motion;

import static utils.Utilities.checkState;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import utils.Indexed;
import utils.func.Funcs;
import utils.func.Tuple;
import utils.stream.FStream;

import jarvey.assoc.Association;
import jarvey.streams.model.TrackletId;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AssociationCollection<T extends Association> implements Iterable<T>  {
	private static final Logger s_logger = LoggerFactory.getLogger(AssociationCollection.class);
	
	private final boolean m_keepBestAssociationOly;
	private final List<T> m_associations;
	
	public AssociationCollection(boolean keepBestAssociationOnly) {
		m_associations = Lists.newArrayList();
		m_keepBestAssociationOly = keepBestAssociationOnly;
	}
	
	AssociationCollection(List<T> associations, boolean keepBestAssociationOnly) {
		m_associations = associations;
		m_keepBestAssociationOly = keepBestAssociationOnly;
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
	public Iterator<T> iterator() {
		return m_associations.iterator();
	}
	
	public List<T> find(TrackletId trkId) {
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
	public T get(Set<TrackletId> key) {
		return Funcs.findFirst(m_associations, a -> key.equals(a.getTracklets()));
	}

	/**
	 * 주어진 tracklet 들로 구성된 association 객체와 collection 내의 순번을 반환한다.
	 *
	 * @param key	검색 키로 사용할 tracklet 집합.
	 * @return	검색된 association 및 순번 객체. 존재하지 않는 경우에는 null.
	 */
	public Indexed<T> getIndexed(Set<TrackletId> key) {
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
	public FStream<T> findAll(TrackletId key) {
		return FStream.from(m_associations)
						.filter(a -> a.containsTracklet(key));
	}
	
	public FStream<Indexed<T>> findIndexedAll(TrackletId key) {
		return FStream.from(m_associations)
						.zipWithIndex()
						.map(t -> Indexed.with(t._1, t._2))
						.filter(idxed -> idxed.value().containsTracklet(key));
	}
	
	public T findSuperiorFirst(AssociationClosure key) {
		return Funcs.findFirst(m_associations, cl -> cl.isSuperior(key));
	}
	
	public boolean add(T assoc) {
		return m_keepBestAssociationOly ? addDisallowConflict(assoc) : addAllowConflict(assoc);
	}
	
	/**
	 * 주어진 tracklet들로 구성된 association을 collection에서 제거한다.
	 *
	 * @param key	tracklet id 집합
	 * @return	제거된 association 객체. 해당 키의 association 존재하지 않은 경우는 {@code null}.
	 */
	public T remove(Set<TrackletId> key) {
		return Funcs.removeFirstIf(m_associations, a -> a.match(key));
	}
	
	public T remove(int index) {
		return m_associations.remove(index);
	}
	
	public List<T> removeInferiors(T key) {
		return Funcs.removeIf(m_associations, cl -> cl.isInferior(key));
	}
	
	private boolean addAllowConflict(T assoc) {
		// collection이 빈 경우는 바로 삽입하고 반환한다.
		if ( size() == 0 ) {
			m_associations.add(assoc);
			return true;
		}
		
		// collection에 이미 추가하려는 association보다 더 specific한 association이
		// 하나라도 이미 존재하는지 확인하여 존재하면 삽입을 취소한다.
		if ( Funcs.exists(m_associations, a -> a.isMoreSpecific(assoc)) ) {
			return false;
		}
		
		// 동일 tracklet으로 구성된 association이 이미 존재하는 경우는
		// score 값만 변경하고, 그렇지 않은 경우는 collection 추가한다.
		Indexed<T> found = getIndexed(assoc.getTracklets());
		if ( found != null ) {
			if ( found.value().getScore() < assoc.getScore() ) {
				m_associations.set(found.index(), assoc);
				return true;
			}
			else {
				return false;
			}
		}
		else {
			// Collection에 포함된 less-speicific한 association들은 모두 제거한다.
			m_associations.removeIf(assoc::isMoreSpecific);	
			m_associations.add(assoc);
			return true;
		}
	}
	
	private boolean addDisallowConflict(T assoc) {
		// collection이 빈 경우는 바로 삽입하고 반환한다.
		if ( size() == 0 ) {
			m_associations.add(assoc);
			return true;
		}
		
//		// FIXME: 나중에 삭제할 것
//		if ( assoc.getTimestamp() == 21400 ) {
//			System.out.print("");
//		}
////		if ( assoc.containsTracklet(TrackletId.fromString("etri:04[3]")) ) {
////			System.out.print("");
////		}
		
		// 새로 삽입할 association에 포함된 tracklet들 중 일부라도 동일한 tracklet을
		// 가진 association들을 구한다.
		List<T> overlaps = Funcs.filter(m_associations, assoc::intersectsTracklet);
        if ( overlaps.isEmpty() ) {
            // 충돌하는 association이 없는 경우는 새 association을 삽입한다.
            boolean done = addAllowConflict(assoc);
            checkState(done);
            
            return true;
        }
        
        //
        // 겹치는 association (overlaps) 들이 존재하는 경우
        //
        
        // 삽입될 assoc보다 더 superior한 association이 이미 존재하면 삽입을 취소한다.
        // (즉, superior한 association 이미 존재하면 취소함.)
        Association superior = Funcs.findFirst(overlaps, c -> c.isSuperior(assoc));
        if ( superior != null ) {
        	if ( s_logger.isDebugEnabled() ) {
        		s_logger.debug("reject adding association: assoc={}, superior={}", assoc, superior);
        	}
        	return false;
        }
      
		//
		// 현재 상태는 새로 삽입될 association이 overlap된 기존 association들보다
        // superior하다고 볼 수 있다.
        //
        
        // overlap에 포함된 association들을 'm_associations'에서 일단 제거한다.
        // (이후에 conflict를 유발하는 binary association만 제거 후 다시 추가시킨다.)
        Funcs.removeIf(m_associations, a -> Funcs.exists(overlaps, ov -> ov.match(a.getTracklets())));
        
        boolean done = m_associations.add(assoc);
        checkState(done);
        
        // 새 association과 conflict를 유발하는 association들에서 새로 삽입된 association과
        // 겹치는 부분을 제외한 association이 존재하는 경우 이것을 다시 삽입시킨다.
        List<T> conflicts = Funcs.filter(overlaps, assoc::isConflict);
        for ( T conflict: conflicts ) {
        	// conlict와 assoc과 공유하는 tracklet을 모두 제거한 뒤,
        	// 남은 association이 존재한다면 이를 삽입한다.
        	Set<TrackletId> commonTrkIds = conflict.intersectionTracklet(assoc);
        	T shrinked = removeTracklets(conflict, commonTrkIds);
    		if ( shrinked != null && shrinked.size() >= 2 ) {
    			addAllowConflict(conflict);
    		}
        }
        
        return true;
	}
	
	public List<T> getBestAssociations() {
		return selectBestAssociations(m_associations);
	}
	
	public static <T extends Association> List<T> selectBestAssociations(List<T> assocList) {
		List<T> bestAssocList = Lists.newArrayList();
		List<T> sorted = FStream.from(assocList)
								.sort(a -> Tuple.of(a.size(), a.getScore()))
								.toList();
		sorted = Lists.reverse(sorted);
		while ( sorted.size() > 0 ) {
			T best = sorted.remove(0);
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
	
	@SuppressWarnings("unchecked")
	private T removeTracklets(T assoc, Iterable<TrackletId> trkIds) {
		T shrinked = assoc;
    	for ( TrackletId trkId: trkIds ) {
    		shrinked = (T)assoc.removeTracklet(trkId);
    		if ( shrinked == null ) {
    			return null;
    		}
    	}
    	return shrinked;
	}
}
