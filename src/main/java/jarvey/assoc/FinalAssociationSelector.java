package jarvey.assoc;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import utils.func.Funcs;
import utils.stream.FStream;

import jarvey.streams.BinaryAssociationCollection;
import jarvey.streams.model.AssociationClosure;
import jarvey.streams.model.BinaryAssociation;
import jarvey.streams.model.TrackletDeleted;
import jarvey.streams.model.TrackletId;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FinalAssociationSelector implements ValueMapper<TrackletDeleted, Iterable<AssociationClosure>> {
	private static final Logger s_logger = LoggerFactory.getLogger(FinalAssociationSelector.class);

	private final BinaryAssociationCollection m_binaryCollection;
	private final AssociationCollection m_collection;
	private final Set<TrackletId> m_closedTracklets;
	
	public FinalAssociationSelector(BinaryAssociationCollection binaryCollection,
											AssociationCollection associations,
											Set<TrackletId> closedTracklets) {
		m_binaryCollection = binaryCollection;
		m_collection = associations;
		m_closedTracklets = closedTracklets;
	}

	@Override
	public Iterable<AssociationClosure> apply(TrackletDeleted deleted) {
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("tracklet deleted: {}", deleted.getTrackletId());
		}
		
		List<AssociationClosure> closedAssociations = handleTrackDeleted(deleted);
		// 최종적으로 선택된 association closure에 포함된 tracklet들과 연관된
		// 모든 binary association들을 제거한다.
		closedAssociations.forEach(this::purgeClosedBinaryAssociation);
		
		if ( closedAssociations.size() > 0 ) {
			return FStream.from(closedAssociations)
							.sort(AssociationClosure::getTimestamp)
							.toList();
		}
		else {
			return Collections.emptyList();
		}
	}
		
	private List<AssociationClosure> handleTrackDeleted(TrackletDeleted deleted) {
		m_closedTracklets.add(deleted.getTrackletId());
		
		// 주어진 tracklet의 delete로 인해 해당 tracklet과 연관된 association들 중에서
		// fully closed된 association만 뽑는다.
		List<AssociationClosure> fullyCloseds
				= Funcs.filter(m_collection, cl -> m_closedTracklets.containsAll(cl.getTracklets()));
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
				// 'closed'보다 superior한 closure가 존재하는 경우
				// 선택하지 않는다.
				if ( s_logger.isDebugEnabled() ) {
					s_logger.debug("found a superior: this={} superior={}", closed, superior);
				}
			}
			else {
				graduated.add(graduate(closed));
			}
		}
		
		return graduated;
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
		}
		m_collection.remove(closure.getTracklets());
		
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("final associations: {}", closure);
		}
		
		return closure;
	}
	
	private void purgeClosedBinaryAssociation(AssociationClosure assoc) {
		// 주어진 tracklet이 포함된 모든 binary association을 제거한다.
		List<BinaryAssociation> purgeds = m_binaryCollection.removeAll(assoc.getTracklets());
		if ( s_logger.isDebugEnabled() && purgeds.size() > 0 ) {
			purgeds.forEach(ba -> s_logger.debug("delete binary-association: {}", ba));
		}
	}
}
