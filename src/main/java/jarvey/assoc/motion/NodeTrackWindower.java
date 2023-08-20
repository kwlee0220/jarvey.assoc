package jarvey.assoc.motion;

import java.util.List;
import java.util.function.BiConsumer;

import com.google.common.collect.Lists;

import utils.stream.FStream;

import jarvey.streams.EventCollectingWindowAggregation;
import jarvey.streams.HoppingWindowManager;
import jarvey.streams.Windowed;
import jarvey.streams.model.Timestamped;
import jarvey.streams.node.NodeTrack;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class NodeTrackWindower implements BiConsumer<String, NodeTrack> {
	private final EventCollectingWindowAggregation<TaggedTrack> m_aggregation;
	private final List<BiConsumer<String,List<NodeTrack>>> m_consumers = Lists.newArrayList();
	
	public NodeTrackWindower(HoppingWindowManager windowMgr) {
		m_aggregation = new EventCollectingWindowAggregation<>(windowMgr);
	}
	
	public void addOutputConsumer(BiConsumer<String,List<NodeTrack>> consumer) {
		m_consumers.add(consumer);
	}

	@Override
	public void accept(String areaId, NodeTrack track) {
		List<Windowed<List<TaggedTrack>>> windoweds = m_aggregation.collect(new TaggedTrack(areaId, track));
		FStream.from(windoweds)
				.flatMapIterable(this::groupByArea)
				.forEach(tagged -> {
					m_consumers.forEach(c -> c.accept(tagged.areaId(), tagged.value()));
				});
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
