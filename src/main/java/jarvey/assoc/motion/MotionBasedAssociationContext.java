package jarvey.assoc.motion;

import java.util.Map;

import com.google.common.collect.Maps;

import jarvey.assoc.AssociationCollection;
import jarvey.assoc.BinaryAssociationStore;
import jarvey.streams.model.AssociationClosure;
import jarvey.streams.model.BinaryAssociation;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
class MotionBasedAssociationContext {
	private AssociationCollection<BinaryAssociation> m_binaryCollection;
	private BinaryAssociationStore m_binaryStore;
	private final Map<String,Session> m_sessions = Maps.newHashMap();
	
	public AssociationCollection<BinaryAssociation> getBinaryAssociationCollection() {
		return m_binaryCollection;
	}
	
	public BinaryAssociationStore getBinaryAssociationStore() {
		return m_binaryStore;
	}
	
	public void setBinaryAssociationStore(BinaryAssociationStore store) {
		m_binaryStore = store;
		m_binaryCollection = store.load();
	}
	
	Session get(String nodeId) {
		return m_sessions.get(nodeId);
	}
	
	Session getOrCreateSession(String nodeId) {
		return m_sessions.computeIfAbsent(nodeId, k -> new Session());
	}
	
	class Session {
		final AssociationCollection<AssociationClosure> m_collection;
		
		Session() {
			m_collection = new AssociationCollection<>(false);
		}
		
		AssociationCollection<BinaryAssociation> getBinaryAssociationCollection() {
			return MotionBasedAssociationContext.this.getBinaryAssociationCollection();
		}
		
		BinaryAssociationStore getBinaryAssociationStore() {
			return MotionBasedAssociationContext.this.getBinaryAssociationStore();
		}
		
		AssociationCollection<AssociationClosure> getAssociationClosureCollection() {
			return m_collection;
		}
	}
}
