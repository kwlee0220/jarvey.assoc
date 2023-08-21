package jarvey.assoc.motion;

import java.util.Map;

import com.google.common.collect.Maps;

import jarvey.streams.MockKeyValueStore;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class MotionBasedAssociatorContext {
	private final Map<String, ? super MockKeyValueStore<?,?>> m_mockStores = Maps.newHashMap();
	private BinaryTrackAssociator m_binaryAssociator;
	private AssociationClosureBuilder m_associationClosureBuilder;
	
	void addMockKeyValueStore(String name, MockKeyValueStore<?,?> store) {
		m_mockStores.put(name, store);
	}
	
	BinaryTrackAssociator getBinaryTrackAssociator() {
		return m_binaryAssociator;
	}
	
	void setBinaryTrackAssociator(BinaryTrackAssociator assoc) {
		m_binaryAssociator = assoc;
	}
	
	AssociationClosureBuilder getAssociationClosureBuilder() {
		return m_associationClosureBuilder;
	}
	
	void setAssociationClosureBuilder(AssociationClosureBuilder builder) {
		m_associationClosureBuilder = builder;
	}
}
