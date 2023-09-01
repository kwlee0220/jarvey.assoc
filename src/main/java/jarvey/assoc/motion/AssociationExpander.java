package jarvey.assoc.motion;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;

import utils.func.Funcs;

import jarvey.assoc.motion.MotionBasedAssociationContext.Session;
import jarvey.streams.model.AssociationClosure;
import jarvey.streams.model.AssociationClosure.Expansion;
import jarvey.streams.model.BinaryAssociation;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class AssociationExpander {
	private final Session m_session;
	
	public AssociationExpander(Session session) {
		m_session = session;
	}
	
	List<AssociationClosure> extend(BinaryAssociation assoc) {
		// assoc을 구성하는 tracklet을 포함하고 있는 모든 closure들을 구한다.
		List<AssociationClosure> matchingClosures = Funcs.removeIf(m_session.m_collection, assoc::intersectsTracklet);
		if ( matchingClosures.isEmpty() ) {
			// 해당 closure가 존재하지 않는 경우는 별도의 conflict 가능성이 없고,
			// extend될 수 있는 closure도 없기 때문에 assoc으로만 구성된 closure를 collection에 추가한다.
			AssociationClosure init = AssociationClosure.from(Arrays.asList(assoc));
			m_session.m_collection.add(init);
			return Arrays.asList(init);
		}
		else {
			List<AssociationClosure> extendeds = Lists.newArrayList();
			for ( AssociationClosure cl: matchingClosures ) {
				Expansion ext = cl.expand(assoc, !m_session.m_collection.getKeepBestAssociationOnly());
				switch ( ext.type() ) {
					case UNCHANGED:
						m_session.m_collection.add(ext.association());
						break;
					case UPDATED:
					case EXTENDED:
						if ( m_session.m_collection.add(ext.association()) ) {
							extendeds.add(ext.association());
						}
						break;
					case CREATED:
						m_session.m_collection.add(cl);
						if ( m_session.m_collection.add(ext.association()) ) {
							extendeds.add(ext.association());
						}
						break;
				}
			}
			return extendeds;
		}
	}
}
