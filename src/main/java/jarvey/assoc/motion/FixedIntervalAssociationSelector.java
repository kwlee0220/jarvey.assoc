package jarvey.assoc.motion;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.streams.kstream.ValueMapper;

import com.google.common.collect.Lists;

import jarvey.assoc.BinaryAssociation;
import jarvey.streams.HoppingWindowManager;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FixedIntervalAssociationSelector
		implements ValueMapper<BinaryAssociation, Iterable<BinaryAssociation>> {
	private final HoppingWindowManager m_windowMgr;
	private final AssociationCollection<BinaryAssociation> m_collection;
	
	public FixedIntervalAssociationSelector(Duration windowSize) {
		m_windowMgr = HoppingWindowManager.ofWindowSize(windowSize);
		m_collection = new AssociationCollection<>(true);
	}

	@Override
	public Iterable<BinaryAssociation> apply(BinaryAssociation assoc) {
		m_collection.add(assoc);
		
		if ( m_windowMgr.collect(assoc.getTimestamp())._1.size() > 0 ) {
			List<BinaryAssociation> pendings = Lists.newArrayList(m_collection);
			m_collection.clear();
			
			return pendings;
		}
		else {
			return Collections.emptyList();
		}
	}
}
