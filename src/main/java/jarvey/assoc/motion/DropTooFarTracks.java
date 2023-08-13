package jarvey.assoc.motion;

import java.util.List;

import org.apache.kafka.streams.kstream.ValueMapper;

import jarvey.assoc.OverlapArea;
import jarvey.assoc.OverlapAreaRegistry;
import jarvey.streams.model.NodeTrack;
import utils.func.Funcs;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class DropTooFarTracks implements ValueMapper<OverlapAreaTagged<List<NodeTrack>>,
												OverlapAreaTagged<List<NodeTrack>>> {
	private final OverlapAreaRegistry m_areaRegistry;
	private int m_dropCount = 0;
	
	public DropTooFarTracks(OverlapAreaRegistry areaRegistry) {
		m_areaRegistry = areaRegistry;
	}

	@Override
	public OverlapAreaTagged<List<NodeTrack>>
	apply(OverlapAreaTagged<List<NodeTrack>> taggedTracks) {
		String areaId = taggedTracks.areaId();
		OverlapArea area = m_areaRegistry.get(areaId);
		
		List<NodeTrack> ntracks = taggedTracks.value();
		List<NodeTrack> closeTracks = Funcs.filter(ntracks, nt -> withinDistance(nt, area));
		return OverlapAreaTagged.tag(closeTracks, areaId);
	}
	
	private boolean withinDistance(NodeTrack track, OverlapArea area) {
		if ( track.isDeleted() ) {
			return true;
		}
		else {
			double threshold = area.getDistanceThreshold(track.getNodeId());
			if ( track.getDistance() <= threshold ) {
				return true;
			}
			else {
//				++m_dropCount;
//				if ( m_dropCount % 10 == 0 ) {
//					System.out.printf("drop count: %d%n", m_dropCount);
//				}
				return false;
			}
		}
	}
}
