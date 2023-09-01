package jarvey.assoc.feature;

import jarvey.assoc.AssociationCollection;
import jarvey.assoc.BinaryAssociationStore;
import jarvey.streams.model.BinaryAssociation;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
class FeatureBasedAssociationContext {
	private AssociationCollection<BinaryAssociation> m_binaryCollection;
	private BinaryAssociationStore m_binaryStore;
	
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
}
