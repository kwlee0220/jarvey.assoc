package jarvey.assoc;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Sets;

import jarvey.streams.model.Timestamped;
import jarvey.streams.model.TrackletId;
import utils.func.Funcs;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface Association extends Timestamped {
	public static enum BinaryRelation {
		DISJOINT,
		MERGEABLE,
		CONFLICT,
	};
	
	/**
	 * Association에 참여된 모든 tracklet id를 반환한다.
	 *
	 * @return	참여 tracklet id 세트.
	 */
	public Set<TrackletId> getTracklets();
	
	/**
	 * Association에 부여된 점수를 반환한다.
	 *
	 * @return	association 점수.
	 */
	public double getScore();
	
	/**
	 * 
	 * @param trkId
	 * @return
	 */
	public Association removeTracklet(TrackletId trkId);
	
	/**
	 * Association에 참여하는 tracklet의 갯수를 반환한다.
	 *
	 * @return	참여 tracklet 갯수.
	 */
	public default int size() {
		return getTracklets().size();
	}
	
	/**
	 * 주어진 node에서 참여한 tracklet의 id를 반환한다.
	 *
	 * @param nodeId	노드 식별자.
	 * @return		해당 노드에서 참여하는 tracklet의 식별자.
	 * 				만일 주어진 노드에 해당하는 tracklet이 없는 경우는 {@code null}를 반환함.
	 */
	public default TrackletId getTrackletId(String nodeId) {
		return Funcs.findFirst(getTracklets(), t -> t.getNodeId().equals(nodeId));
	}
	
	/**
	 * 본 association을 구성하는 tracklet들과 주어진 key와 동일 여부를 반환한다.
	 * 
	 * @param key	tracklet id 집합.
	 * @return	key와 동일한 tracklet으로 구성된 경우에는 {@code true},
	 * 			그렇지 않은 경우에는 {@code false}.
	 */
	public default boolean match(Set<TrackletId> key) {
		return getTracklets().equals(key);
	}
	
	/**
	 * 주어진 tracklet이 본 association에 포함되는지 여부를 반환한다.
	 * 
	 * @param trackletId	{@link TrackletId} 객체.
	 * @return	주어진 {@code trackletId}가 포함된 경우는 {@code true}, 그렇지 않은 경우는 {@code false}.
	 */
	public default boolean containsTracklet(TrackletId trackletId) {
		return Funcs.exists(getTracklets(), t -> t.equals(trackletId));
	}
	
	/**
	 * 주어진 node에 생성된 tracklet이 본 association이 포함되었는지 여부를 반환한다.
	 * 
	 * @param nodeId	노드 식별자.
	 * @return	주어진 {@code nodeId}가 포함된 경우는 {@code true}, 그렇지 않은 경우는 {@code false}.
	 */
	public default boolean containsNode(String nodeId) {
		return Funcs.exists(getTracklets(), t -> t.getNodeId().equals(nodeId));
	}
	
	/**
	 * Association에 참여하는 모든 노드들의 식별자를 반환한다.
	 *
	 * @return	노드 식별자 집합.
	 */
	public default Set<String> getNodes() {
		return Funcs.map(getTracklets(), TrackletId::getNodeId);
	}
	
	/**
	 * 두 association 사이의 관계를 반환한다.
	 * 두 association 사이의 관계를 아래와 같다.
	 * <dl>
	 * 	<dt>{@link BinaryRelation#DISJOINT}:</dt>
	 * 	<dd>두 association을 구성하는 tracklet id들 사이에 교집합이 없는 경우.</dd> 
	 * 	<dt>{@link BinaryRelation#MERGEABLE}:</dt>
	 * 	<dd>두 association을 구성하는 tracklet id들 사이에 교집합을 제외한 tracklet id 간에
	 * 		동일 node의 다른 tracklet id가 존재하지 않는 경우.</dd> 
	 * 	<dt>{@link BinaryRelation#CONFLICT}:</dt>
	 * 	<dd>두 association을 구성하는 tracklet id들 사이에 교집합을 제외한 tracklet id 간에
	 * 		동일 node의 다른 tracklet id가 존재하는 경우.</dd> 
	 * </dl>
	 * 
	 * @param other
	 * @return
	 */
	public default BinaryRelation relate(Association other) {
		Set<TrackletId> overlap = Sets.intersection(getTracklets(), other.getTracklets());
		if ( overlap.isEmpty() ) {
			return BinaryRelation.DISJOINT;
		}

		Set<TrackletId> left = Sets.difference(getTracklets(), overlap);
		if ( left.isEmpty() ) {
			return BinaryRelation.MERGEABLE;
		}
		
		Set<TrackletId> right = Sets.difference(other.getTracklets(), overlap);
		if ( right.isEmpty() ) {
			return BinaryRelation.MERGEABLE;
		}
		
		Set<String> leftNodes = Funcs.map(left, TrackletId::getNodeId);
		boolean conflict = FStream.from(right).exists(rid -> leftNodes.contains(rid.getNodeId()));
		return conflict ? BinaryRelation.CONFLICT : BinaryRelation.MERGEABLE;
	}
	
	/**
	 * 두 association이 서로 conflict 여부를 반환한다.
	 *
	 * @param other		비교 대상 association
	 * @return		Conflict인 경우는 true 그렇지 않은 경우는 false
	 */
	public default boolean isConflict(Association other) {
		return relate(other) == BinaryRelation.CONFLICT;
	}

	/**
	 * 두 association이 서로 compatible 여부를 반환한다.
	 * 두 association 사이에 최소 1개 이상의 동일 node의 track이 존재하고,
	 * 모든 동일 node의 track 식별자가 동일한 경우 compatible함.
	 *
	 * @param other		비교 대상 association
	 * @return		Compatible한 경우는 True 그렇지 않은 경우는 False.
	 */
	public default boolean isMergeable(Association other) {
		return relate(other) == BinaryRelation.MERGEABLE;
	}
	
	/**
	 * 두 association이 서로 disjoint 여부를 반환한다.
	 * 두 association 사이의 동일 tracklet를 포함하지 않는 경우를 disjoint라고 정의함.
	 *
	 * @param other		비교 대상 association
	 * @return		disjoin 여부.
	 */
	public default boolean isDisjoint(Association other) {
		return Collections.disjoint(getTracklets(), other.getTracklets());
	}
	
	/**
	 * 본 association이 주어진 assoc보다 많은 정보가 포함되었는지 여부를 반환한다.
	 *
	 * @param assoc		대상 association 객체.
	 * @return		구체화 여부.
	 */
	public default boolean isMoreSpecific(Association assoc) {
		// 본 association을 구성하는 tracklet의 수가 'assoc'의 tracklet 수보다 작다면
		// 'more-specific'일 수 없기 때문에 'false'를 반환한다.
		if ( size() < assoc.size() ) {
			return false;
		}

		// 'assoc'에만 포함된 tracklet이 존재하면 more-specific하다고 할 수 없기 때문에
		// false를 반환한다.
		if ( !getTracklets().containsAll(assoc.getTracklets()) ) {
			return false;
		}
		
		//
		// 본 association tracklet 집합이 assoc의 tracklet 집합과 동일하던지 superset 이다.
		//
		if ( size() > assoc.size() ) {
			// 참여된 tracklet 갯수가 더 많으면 더이상 확인할 필요없이 specific하다.
			return true;
		}
		else {
			// this와 assoc은 서로 동일한 tracklet으로 구성된 closure인 경우
			return getScore() > assoc.getScore();
		}
	}
	
	/**
	 * 두 association 사이의 우월성 여부를 반환한다.
	 * 
	 * Association A가 association B보다 다음과 같은 경우에 우월하다고 정의한다.
	 * <ul>
	 * 	<li> A가 B가 conflict한 경우에는 A가 B보다 score가 높다.
	 * 	<li> A와 B가 compatible한 경우에는 A가 B보다 specific하다.
	 * </ul>
	 * 
	 * @param other		대상 association.
	 * @return
	 */
	public default boolean isSuperior(Association other) {
		switch ( relate(other )) {
			case CONFLICT:
				if ( size() < other.size() ) {
					return false;
				}
				else {
					return getScore() > other.getScore();
				}
			case MERGEABLE:
				return isMoreSpecific(other);
			case DISJOINT:
				return false;
			default:
				throw new AssertionError();
		}
	}
	
	public default boolean isInferior(Association other) {
		switch ( relate(other )) {
			case CONFLICT:
				if ( size() > other.size() ) {
					return false;
				}
				else {
					return getScore() < other.getScore();
				}
			case MERGEABLE:
				return other.isMoreSpecific(this);
			case DISJOINT:
				return false;
			default:
				throw new AssertionError();
		}
	}
	
	public default boolean intersectsTracklet(Association assoc) {
		return Funcs.intersects(getTracklets(), assoc.getTracklets());
	}
	
	public default boolean intersectsNode(Association assoc) {
		return Funcs.intersects(getNodes(), assoc.getNodes());
	}
	
	public default Set<TrackletId> intersectionTracklet(Association assoc) {
		return Sets.intersection(getTracklets(), assoc.getTracklets());
	}
}