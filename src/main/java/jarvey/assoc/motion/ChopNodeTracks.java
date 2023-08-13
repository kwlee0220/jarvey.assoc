package jarvey.assoc.motion;

import java.util.List;

import org.apache.kafka.streams.kstream.ValueMapperWithKey;

import jarvey.streams.EventCollectingAggregation;
import jarvey.streams.HoppingWindowManager;
import jarvey.streams.Windowed;
import jarvey.streams.model.NodeTrack;
import jarvey.streams.model.Timestamped;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ChopNodeTracks implements ValueMapperWithKey<String, NodeTrack,
												Iterable<OverlapAreaTagged<List<NodeTrack>>>> {
	private final EventCollectingAggregation<TaggedTrack> m_aggregation;
	
	public ChopNodeTracks(HoppingWindowManager windowMgr) {
		m_aggregation = new EventCollectingAggregation<>(windowMgr);
	}

	@Override
	public Iterable<OverlapAreaTagged<List<NodeTrack>>>
	apply(String areaId, NodeTrack track) {
		List<Windowed<List<TaggedTrack>>> windoweds = m_aggregation.collect(new TaggedTrack(areaId, track));
		
		return FStream.from(windoweds)
						.flatMapIterable(this::groupByArea)
						.toList();
	}
	
	private List<OverlapAreaTagged<List<NodeTrack>>>
	groupByArea(Windowed<List<TaggedTrack>> wtaggeds) {
		return FStream.from(wtaggeds.value())
						.groupByKey(TaggedTrack::area, TaggedTrack::track)
						.stream()
						.map((a, bkt) -> OverlapAreaTagged.tag(bkt, a))
						.toList();
	}
	
	private static final class TaggedTrack implements Timestamped {
		private final String m_areaId;
		private final NodeTrack m_track;
		
		private TaggedTrack(String areaId, NodeTrack track) {
			m_areaId = areaId;
			m_track = track;
		}
		
		public String area() {
			return m_areaId;
		}
		
		public NodeTrack track() {
			return m_track;
		}

		@Override
		public long getTimestamp() {
			return m_track.getTimestamp();
		}
	}
}
