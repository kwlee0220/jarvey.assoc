package jarvey.assoc.feature;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import utils.func.Funcs;
import utils.stream.FStream;

import jarvey.assoc.AssociationCollection;
import jarvey.streams.model.AssociationClosure;
import jarvey.streams.model.BinaryAssociation;
import jarvey.streams.model.TrackletDeleted;
import jarvey.streams.model.TrackletId;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class FinalAssociationSelector implements ValueMapperWithKey<String, TrackletDeleted,
																		Iterable<AssociationClosure>> {
	private static final Logger s_logger = LoggerFactory.getLogger(FinalAssociationSelector.class);

	private final FeatureBasedAssociationContext m_context;
	private final Set<String> m_listeningNodes;
	
	private final AssociationCollection<AssociationClosure> m_collection;
	private final Map<TrackletId,TrackletDeleted> m_closedTracklets = Maps.newHashMap();
	private final Set<AssociationClosure> m_pendingFullClosers = Sets.newHashSet();
	
	public FinalAssociationSelector(FeatureBasedAssociationContext context, Set<String> listeningNodes,
									AssociationCollection<AssociationClosure> collection) {
		m_context = context;
		m_listeningNodes = listeningNodes;
		m_collection = collection;
	}

	@Override
	public Iterable<AssociationClosure> apply(String nodeId, TrackletDeleted deleted) {
		m_closedTracklets.put(deleted.getTrackletId(), deleted);
		m_context.getBinaryAssociationStore().markTrackletClosed(deleted.getTrackletId());
		
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("tracklet deleted: {}", deleted.getTrackletId());
		}
		
		List<AssociationClosure> fixedClosures = handleTrackDeleted(deleted);
		if ( fixedClosures.size() > 0 ) {
			Set<TrackletId> closedTracklets = FStream.from(fixedClosures)
													.flatMapIterable(cl -> cl.getTracklets())
													.toSet();
			
			// 최종적으로 선택된 association closure에 포함된 tracklet들과 연관된
			// 모든 binary association들을 제거한다.
			purgeClosedBinaryAssociation(closedTracklets);
			
			// dangling tracklet들을 찾아 제거한다.
			removeDanglingClosedTracklets(deleted.getTimestamp());
			
			return FStream.from(fixedClosures)
							.sort(AssociationClosure::getTimestamp)
							.toList();
		}
		else {
			return Collections.emptyList();
		}
	}
	
	private boolean isFullyClosed(AssociationClosure closure, Set<TrackletId> closedTracklets) {
		return !FStream.from(closure.getTracklets())
						.filter(trk -> m_listeningNodes.contains(trk.getNodeId()))
						.filter(trk -> !closedTracklets.contains(trk))
						.findFirst()
						.isPresent();
	}
	
	private List<AssociationClosure> handleTrackDeleted(TrackletDeleted deleted) {
		final TrackletId deleteTrkId = deleted.getTrackletId();
		
		// 종료된 tracklet과 연관되었던 모든 peer tracklet-id를 찾는다.
		Set<TrackletId> peers = findPeerTrackletIds(deleteTrkId);
		
		// 찾아진 peer tracklet-id가 본 session node의 다른 competing tracklet과도 연관이 존재하는지
		// 검사한다.
		Set<TrackletId> competitors = FStream.from(peers)
											.flatMapIterable(this::findPeerTrackletIds)
											.filter(k -> k.getNodeId().equals(deleteTrkId.getNodeId()))
											.filterNot(deleteTrkId::equals)
											.toSet();
		
		final Set<TrackletId> closedTracklets = m_closedTracklets.keySet();

		// 경쟁하는 다른 tracklet이 존재한다면 그 tracklet들이 모두 종료할 때까지
		// 최종 결정을 미룬다.
		if ( Sets.difference(competitors, closedTracklets).size() > 0 ) {
			return Collections.emptyList();
		}
		
		//
		// delete된 tracklet과 연관된 모든 tracklet이 종료되었기 때문에
		// 최종 association closure를 계산한다.
		//
		
		// 주어진 tracklet의 delete로 인해 해당 tracklet과 연관된 association들 중에서
		// fully closed된 association만 뽑는다.
		List<AssociationClosure> fullyCloseds = Funcs.filter(m_collection,
															cl -> isFullyClosed(cl, closedTracklets));
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
				m_pendingFullClosers.add(closed);
			}
			else {
				graduate(m_collection, closed);
				graduated.add(closed);
			}
		}
		
		// pending되어 있던 closure 중에서 삭제를 막고 있던 closure가 또 다른 fully-closed
		// closure에 의해 삭제되었을 수도 있기 때문에, 삭제 여부를 확인한다.
		if ( graduated.size() > 0 && m_pendingFullClosers.size() > 0) {
			Funcs.removeIf(m_pendingFullClosers, fc -> {
				if ( m_collection.findSuperiorFirst(fc) == null ) {
					graduate(m_collection, fc);
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
	
	private void graduate(AssociationCollection<AssociationClosure> collection, AssociationClosure closure) {
		// 졸업할 closure보다 inferior한 모든 closure들을 제거한다.
		List<AssociationClosure> removeds = collection.removeInferiors(closure);
		if ( removeds.size() > 0 ) {
			if ( s_logger.isDebugEnabled() ) {
				for ( AssociationClosure cl: removeds ) {
					s_logger.debug("removed an inferior for the graduated closure: removed={} superior={}", cl, closure);
				}
			}
				
			// 이 삭제로 pending list에 있는 closure도 삭제될 수 있기 때문에, pending list에서도 제거한다.
			m_pendingFullClosers.removeAll(removeds);
		}
		
		// graduate시키는 closure가 이전에 superior 때문에 graduate하지 못하고
		// m_pendingFullClosers에 포함되어 있을 수 있기 때문에 여기서도 제거한다.
		m_pendingFullClosers.remove(closure);
		
		collection.remove(closure.getTracklets());
	}
	
	private void purgeClosedBinaryAssociation(Set<TrackletId> trkIds) {
		// 주어진 tracklet이 포함된 모든 binary association을 제거한다.
		List<BinaryAssociation> purgeds = Funcs.removeIf(m_context.getBinaryAssociationCollection(),
														ba -> Funcs.intersects(ba.getTracklets(), trkIds));
		if ( s_logger.isDebugEnabled() ) {
			purgeds.forEach(ba -> s_logger.debug("delete binary-association: {}", ba));
		}
		trkIds.forEach(m_context.getBinaryAssociationStore()::removeRecord);
		
		trkIds.forEach(m_closedTracklets::remove);
		if ( s_logger.isDebugEnabled() ) {
			trkIds.forEach(trkId -> s_logger.debug("delete tracklet: {}", trkId));
		}
	}
	
	private List<TrackletDeleted> removeDanglingClosedTracklets(long ts) {
		long minExitTs = ts - Duration.ofSeconds(7).toMillis();
		
		// close된 tracklet들 중에서 binary association이 없는 경우
		// 해당 tracklet은 다른 tracklet과 association 없이 종료된 것으로
		// 단일 association으로 구성된 closure를 생성한다.
		List<utils.func.KeyValue<TrackletId,TrackletDeleted>> danglings
				= Funcs.removeIf(m_closedTracklets, (tid, ev) -> isDanglingTracklet(ev, minExitTs));
		if ( s_logger.isDebugEnabled() && danglings.size() > 0 ) {
			FStream.from(danglings)
					.map(kv -> kv.key())
					.forEach(trkId -> s_logger.debug("delete dangling tracklet: {}", trkId));
		}
		
		return Funcs.map(danglings, kv -> kv.value());
	}
	
	private boolean isDanglingTracklet(TrackletDeleted deleted, long minExitTs) {
		if ( deleted.getTimestamp() < minExitTs ) {
			return !Funcs.exists(m_context.getBinaryAssociationCollection(),
								ba -> ba.containsTracklet(deleted.getTrackletId()));
		}
		else {
			return false;
		}
	}
}
