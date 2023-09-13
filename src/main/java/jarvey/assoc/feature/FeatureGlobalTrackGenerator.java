package jarvey.assoc.feature;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import utils.func.Funcs;

import jarvey.assoc.AssociationCollection;
import jarvey.assoc.OverlapAreaRegistry;
import jarvey.streams.model.Association;
import jarvey.streams.model.AssociationClosure;
import jarvey.streams.model.GlobalTrack;
import jarvey.streams.model.LocalTrack;
import jarvey.streams.node.NodeTrack;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FeatureGlobalTrackGenerator implements KeyValueMapper<String, NodeTrack,
																Iterable<KeyValue<String,GlobalTrack>>> {
	private final OverlapAreaRegistry m_areaRegistry;
	private final Set<String> m_listeningNodes;
	private final AssociationCollection m_collection;
	
	public FeatureGlobalTrackGenerator(OverlapAreaRegistry areaRegistry, Set<String> listeningNodes,
										AssociationCollection associations) {
		m_areaRegistry = areaRegistry;
		m_listeningNodes = listeningNodes;
		m_collection = associations;
	}

	@Override
	public Iterable<KeyValue<String,GlobalTrack>> apply(String nodeId, NodeTrack ntrack) {
		LocalTrack ltrack = LocalTrack.from(ntrack);
		
		if ( !m_listeningNodes.contains(ntrack.getNodeId()) ) {
			return Collections.emptyList();
		}
		
		// 현재까지의 association들 중에서 superior들만 추린다.
		List<AssociationClosure> bestAssocs = m_collection.getBestAssociations();
		
		Association assoc = findAssociation(ltrack, bestAssocs);
		GlobalTrack gtrack = (assoc != null)
								? GlobalTrack.from(assoc, ltrack, null)
								: GlobalTrack.from(ltrack, null);
		
		if ( ntrack.getNodeId().equals("etri:04") && ntrack.getTrackId().equals("6") ) {
			System.out.println(gtrack);
		}
		
		return Collections.singletonList(KeyValue.pair(nodeId, gtrack));
	}
	
	private Association findAssociation(LocalTrack ltrack, List<AssociationClosure> associations) {
		return Funcs.findFirst(associations, a -> a.containsTracklet(ltrack.getTrackletId()));
	}
}
