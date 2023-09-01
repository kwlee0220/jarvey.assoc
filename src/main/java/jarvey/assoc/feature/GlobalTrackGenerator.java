package jarvey.assoc.feature;

import java.util.List;

import org.apache.kafka.streams.kstream.ValueMapperWithKey;

import utils.func.Funcs;

import jarvey.assoc.AssociationCollection;
import jarvey.streams.model.AssociationClosure;
import jarvey.streams.model.GlobalTrack;
import jarvey.streams.model.LocalTrack;
import jarvey.streams.node.NodeTrack;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GlobalTrackGenerator implements ValueMapperWithKey<String, NodeTrack, GlobalTrack> {
	private final AssociationCollection<AssociationClosure> m_collection;
	
	public GlobalTrackGenerator(AssociationCollection<AssociationClosure> collection) {
		m_collection = collection;
	}

	@Override
	public GlobalTrack apply(String areaId, NodeTrack ntrack) {
		List<AssociationClosure> assocList = m_collection.find(ntrack.getTrackletId());
		List<AssociationClosure> bestAssociations = AssociationCollection.selectBestAssociations(assocList);
		AssociationClosure assoc = Funcs.max(bestAssociations, AssociationClosure::getScore);

		LocalTrack ltrack = LocalTrack.from(ntrack);
		if ( assoc != null ) {
			GlobalTrack gtrack = GlobalTrack.from(assoc, ltrack, null);
			System.out.printf("\tassoc=%s, gtrack=%s%n", assoc, gtrack);
			return gtrack;
		}
		else {
			return GlobalTrack.from(ltrack, null);
		}
	}
}
