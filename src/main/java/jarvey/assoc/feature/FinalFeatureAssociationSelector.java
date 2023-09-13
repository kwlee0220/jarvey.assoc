package jarvey.assoc.feature;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import utils.func.Funcs;
import utils.stream.FStream;

import jarvey.assoc.AssociationCollection;
import jarvey.streams.model.AssociationClosure;
import jarvey.streams.model.BinaryAssociation;
import jarvey.streams.model.BinaryAssociationCollection;
import jarvey.streams.model.TrackletDeleted;
import jarvey.streams.model.TrackletId;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class FinalFeatureAssociationSelector implements ValueMapperWithKey<String, TrackletDeleted,
																		Iterable<AssociationClosure>> {
	private static final Logger s_logger = LoggerFactory.getLogger(FinalFeatureAssociationSelector.class);

	private final BinaryAssociationCollection m_binaryCollection;
	private final AssociationCollection m_collection;
	private final Set<TrackletId> m_closedTracklets;
	private final Set<AssociationClosure> m_pendingClosedAssociations = Sets.newHashSet();
	
	public FinalFeatureAssociationSelector(BinaryAssociationCollection binaryCollection,
											AssociationCollection associations,
											Set<TrackletId> closedTracklets) {
		m_binaryCollection = binaryCollection;
		m_collection = associations;
		m_closedTracklets = closedTracklets;
	}

	@Override
	public Iterable<AssociationClosure> apply(String nodeId, TrackletDeleted deleted) {
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("tracklet deleted: {}", deleted.getTrackletId());
		}
		
		List<AssociationClosure> closedClosures = handleTrackDeleted(deleted);
		if ( closedClosures.size() > 0 ) {
			// 최종적으로 선택된 association closure에 포함된 tracklet들과 연관된
			// 모든 binary association들을 제거한다.
			closedClosures.forEach(this::purgeClosedBinaryAssociation);
			
			return FStream.from(closedClosures)
							.sort(AssociationClosure::getTimestamp)
							.toList();
		}
		else {
			return Collections.emptyList();
		}
	}
	
	private List<AssociationClosure> handleTrackDeleted(TrackletDeleted deleted) {
		final TrackletId deleteTrkId = deleted.getTrackletId();
		
		m_closedTracklets.add(deleteTrkId);
		
		// 종료된 tracklet과 연관되었던 모든 peer tracklet-id를 찾는다.
		Set<TrackletId> peers = findPeerTrackletIds(deleteTrkId);
		if ( peers.isEmpty() ) {
			return Collections.emptyList();
		}
		
		// 찾아진 peer tracklet-id가 본 session외에도 동일 listening node의
		// 다른 competing tracklet과도 연관이 존재하는지 검사한다.
		Set<TrackletId> competitors = FStream.from(peers)
											.flatMapIterable(this::findPeerTrackletIds)
											.filter(k -> k.getNodeId().equals(deleteTrkId.getNodeId()))
											.filterNot(deleteTrkId::equals)
											.toSet();

		// 경쟁하는 다른 tracklet이 존재한다면 그 tracklet들이 모두 종료할 때까지
		// 최종 결정을 미룬다.
		if ( !m_closedTracklets.containsAll(competitors) ) {
			return Collections.emptyList();
		}
		
		//
		// delete된 tracklet과 연관된 모든 tracklet이 종료되었기 때문에
		// 최종 association closure를 계산한다.
		//
		
		// 주어진 tracklet의 delete로 인해 해당 tracklet과 연관된 association들 중에서
		// fully closed된 association만 뽑는다.
		List<AssociationClosure> fullyCloseds = Funcs.filter(m_collection,
															cl -> m_closedTracklets.containsAll(cl.getTracklets()));
		// 뽑은 association들 중 일부는 서로 conflict한 것들이 있을 수 있기 때문에
		// 이들 중에서 점수를 기준으로 conflict association들을 제거한 best association 집합을 구한다.
		fullyCloseds = AssociationCollection.selectBestAssociations(fullyCloseds);
		if ( s_logger.isDebugEnabled() && fullyCloseds.size() > 0 ) { 
			for ( AssociationClosure cl: fullyCloseds ) {
				s_logger.debug("fully-closed: {}", cl);
			}
		}
		
		// 만일 유지 중인 association들 중에서 fully-closed 상태가 아니지만,
		// best association보다 superior한 것이 존재하면 해당 best association의 graduation을 대기시킴.
		List<AssociationClosure> graduated = Lists.newArrayList();
		while ( fullyCloseds.size() > 0 ) {
			AssociationClosure closed = Funcs.removeFirst(fullyCloseds);
			
			// close된 closure보다 더 superior한 closure가 있는지 확인하여
			// 없는 경우에만 관련 closure 삭제를 수행한다.
			AssociationClosure superior = m_collection.findSuperiorFirst(closed);
			if ( superior != null ) {
				if ( s_logger.isDebugEnabled() ) {
					s_logger.debug("found a superior: this={} superior={}", closed, superior);
				}
				
				// 'closed'보다 superior한 closure들이 최종 결과가 결정될 때까지 대기한다.
				// 더 superior한 closure가 close될 때는 그 closure가 처리될 때 자동적으로
				// 이 closure가 제거될 것이고, 만일 다른 fully-closed closure로 인해
				// 제거되는 경우를 별도 처리하기 위해 일단 따로 모은다.
				m_pendingClosedAssociations.add(closed);
			}
			else {
				graduated.add(graduate(closed));
			}
		}
		
		// pending되어 있던 closure 중에서 삭제를 막고 있던 closure가 또 다른 fully-closed
		// closure에 의해 삭제되었을 수도 있기 때문에, 삭제 여부를 확인한다.
		if ( graduated.size() > 0 && m_pendingClosedAssociations.size() > 0) {
			Funcs.removeIf(m_pendingClosedAssociations, fc -> {
				if ( m_collection.findSuperiorFirst(fc) == null ) {
					graduate(fc);
					graduated.add(fc);
					return true;
				}
				else {
					return false;
				}
			});
		}
		
		return graduated;
	}

	private Set<TrackletId> findPeerTrackletIds(TrackletId trkId) {
		Set<TrackletId> peers = FStream.from(m_collection)
										.filter(cl -> cl.getTracklets().contains(trkId))
										.flatMapIterable(AssociationClosure::getTracklets)
										.toSet();
		peers.remove(trkId);
		return peers;
	}

	private AssociationClosure graduate(AssociationClosure closure) {
		// 졸업할 closure보다 inferior한 모든 closure들을 제거한다.
		List<AssociationClosure> inferiors = m_collection.removeInferiors(closure);
		if ( inferiors.size() > 0 ) {
			if ( s_logger.isDebugEnabled() ) {
				for ( AssociationClosure cl: inferiors ) {
					s_logger.debug("removed an inferior for the graduated closure: removed={} superior={}",
									cl, closure);
				}
			}
				
			// 이 삭제로 pending list에 있는 closure도 삭제될 수 있기 때문에, pending list에서도 제거한다.
			m_pendingClosedAssociations.removeAll(inferiors);
		}
		
		m_collection.remove(closure.getTracklets());
		
		// graduate시키는 closure가 이전에 superior 때문에 graduate하지 못하고
		// m_pendingFullClosers에 포함되어 있을 수 있기 때문에 여기서도 제거한다.
		m_pendingClosedAssociations.remove(closure);
		
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("final associations: {}", closure);
		}
		
		return closure;
	}
	
	private void purgeClosedBinaryAssociation(AssociationClosure assoc) {
		// 주어진 tracklet이 포함된 모든 binary association을 제거한다.
		List<BinaryAssociation> purgeds = Funcs.removeIf(m_binaryCollection,
														ba -> assoc.getTracklets().containsAll(ba.getTracklets()));
		if ( s_logger.isDebugEnabled() && purgeds.size() > 0 ) {
			purgeds.forEach(ba -> s_logger.debug("delete binary-association: {}", ba));
		}
	}
}
