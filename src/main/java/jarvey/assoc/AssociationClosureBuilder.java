package jarvey.assoc;

import java.util.Collections;

import org.apache.kafka.streams.kstream.ValueMapperWithKey;

import jarvey.streams.model.AssociationClosure;
import jarvey.streams.model.BinaryAssociation;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AssociationClosureBuilder implements ValueMapperWithKey<String, BinaryAssociation,
																		Iterable<AssociationClosure>> {
	private final AssociationCollection m_collection;
	
	public AssociationClosureBuilder(AssociationCollection collection) {
		m_collection = collection;
	}
	
	public AssociationCollection getAssociationCollection() {
		return m_collection;
	}

	@Override
	public Iterable<AssociationClosure> apply(String nodeId, BinaryAssociation assoc) {
		return build(assoc);
	}

	public Iterable<AssociationClosure> build(BinaryAssociation assoc) {
		AssociationClosure cl = AssociationClosure.from(Collections.singletonList(assoc));
		return m_collection.add(cl);
	}
}
