package jarvey.assoc.motion;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import utils.func.Either;
import utils.func.Tuple;
import utils.stream.FStream;

import jarvey.streams.BinaryAssociationCollection;
import jarvey.streams.EventCollectingWindowAggregation;
import jarvey.streams.HoppingWindowManager;
import jarvey.streams.Windowed;
import jarvey.streams.model.BinaryAssociation;
import jarvey.streams.model.Timestamped;
import jarvey.streams.model.TrackletDeleted;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrack;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MotionBinaryTrackletAssociator
		implements KeyValueMapper<String, NodeTrack,
								Iterable<KeyValue<String, Either<BinaryAssociation, TrackletDeleted>>>> {
	@SuppressWarnings("unused")
	private static final Logger s_logger = LoggerFactory.getLogger(MotionBinaryTrackletAssociator.class);
	
	private final EventCollectingWindowAggregation<TaggedTrack> m_aggregation;
	private double m_trackDistanceThreshold;
	private final BinaryAssociationCollection m_binaryCollection;
	
	public MotionBinaryTrackletAssociator(Duration splitInterval, double trackDistanceThreshold,
											BinaryAssociationCollection binaryCollection) {
		HoppingWindowManager windowMgr = HoppingWindowManager.ofWindowSize(splitInterval);
//																.graceTime(Duration.ofSeconds(2));
		m_aggregation = new EventCollectingWindowAggregation<>(windowMgr);
		
		m_trackDistanceThreshold = trackDistanceThreshold;
		m_binaryCollection = binaryCollection;
	}

	@Override
	public Iterable<KeyValue<String, Either<BinaryAssociation, TrackletDeleted>>>
	apply(String areaId, NodeTrack track) {
		List<KeyValue<String, Either<BinaryAssociation, TrackletDeleted>>> results = Lists.newArrayList();
		
		List<Windowed<List<TaggedTrack>>> windoweds = m_aggregation.collect(new TaggedTrack(areaId, track));
		for ( KeyValue<String,List<NodeTrack>> tracksPerArea: FStream.from(windoweds)
																	.flatMapIterable(this::groupByArea) ) {
			String bucketAreaId = tracksPerArea.key;
			List<NodeTrack> bucket = tracksPerArea.value;
			
			FStream.from(associate(bucket))
					// bucket 단위에서 association을 실시하면 결과 association들이 시간순으로
					// 정렬되지 않을 수도 있기 때문에, 이들을 시간 순서대로 정렬시킨다.
					// 이때, TrackletDelete 이벤트로 함께 포함시켜서 정렬시킨다.ㅏ
					.sort(either -> 
						either.isLeft() ? either.getLeft().getTimestamp() : either.getRight().getTimestamp()
					)
					.map(either -> KeyValue.pair(bucketAreaId, either))
					.forEach(results::add);
		}
		
		return results;
	}
	
	private List<KeyValue<String,List<NodeTrack>>>
	groupByArea(Windowed<List<TaggedTrack>> wtaggeds) {
		return FStream.from(wtaggeds.value())
						.groupByKey(TaggedTrack::area, TaggedTrack::track)
						.stream()
						.map((a, bkt) -> KeyValue.pair(a, bkt))
						.toList();
	}
	
	private List<Either<BinaryAssociation,TrackletDeleted>>
	associate(List<NodeTrack> tracks) {
		List<Either<BinaryAssociation,TrackletDeleted>> outEvents = Lists.newArrayList();
		
		Map<TrackletId,TrackletDeleted> deleteds
			= FStream.from(tracks)
					.filter(NodeTrack::isDeleted)
					.toMap(NodeTrack::getTrackletId,
							trk -> TrackletDeleted.of(trk.getTrackletId(), trk.getTimestamp()));
		
		List<TrackSlot> slots = FStream.from(tracks)
										.filterNot(NodeTrack::isDeleted)
										.groupByKey(NodeTrack::getTrackletId)
										.stream()
										.map(TrackSlot::new)
										.toList();
		while ( slots.size() >= 2 ) {
			TrackSlot left = slots.remove(0);
			for ( TrackSlot right: slots ) {
				if ( !left.getNodeId().equals(right.getNodeId()) ) {
					BinaryAssociation assoc = TrackSlot.associate(left, right, m_trackDistanceThreshold);
					if ( assoc != null ) {
						if ( m_binaryCollection.add(assoc) ) {
							outEvents.add(Either.left(assoc));
						}
					}
				}
			}
		}
		
		// delete 이벤트는 마지막으로 추가한다.
		deleteds.forEach((trkId, deleted) -> outEvents.add(Either.right(deleted)));
		
		return outEvents;
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
		
		@Override
		public String toString() {
			return m_track.toString();
		}
	}
	
	private static class TrackSlot {
		private final TrackletId m_trackletId;
		private final List<NodeTrack> m_tracks;
		
		TrackSlot(TrackletId trkId, List<NodeTrack> tracks) {
			m_trackletId = trkId;
			m_tracks = tracks;
		}
		
		public String getNodeId() {
			return m_trackletId.getNodeId();
		}
		
		@Override
		public String toString() {
			return String.format("%s: %s", m_trackletId, m_tracks);
		}
		
		
		private static BinaryAssociation associate(TrackSlot left, TrackSlot right, double distThreshold) {
			TrackletId leftKey = left.m_trackletId;
			TrackletId rightKey = right.m_trackletId;
			
			// 두 track 집합 사이의 score를 계산한다.
			//
			Tuple<Double,Tuple<Long,Long>> ret = calcDistance(left.m_tracks, right.m_tracks);
			if ( ret._1 < distThreshold ) {
				double score = 1 - (ret._1 / distThreshold);
				
				long leftFirstTs = left.m_tracks.get(0).getFirstTimestamp();
				long rightFirstTs = right.m_tracks.get(0).getFirstTimestamp();
				
				return new BinaryAssociation(leftKey, leftFirstTs, ret._2._1,
												rightKey, rightFirstTs, ret._2._2, score);
			}
			else {
				return null;
			}
		}
		
		private static Tuple<Double,Tuple<Long,Long>>
		calcDistance(List<NodeTrack> tracks1, List<NodeTrack> tracks2) {
			List<NodeTrack> longPath, shortPath;
			if ( tracks1.size() >= tracks2.size() ) {
				longPath = tracks1;
				shortPath = tracks2;
			}
			else {
				longPath = tracks2;
				shortPath = tracks1;
			}
			
			return FStream.from(longPath)
							.buffer(shortPath.size(), 1)
							.map(path -> calcSplitDistance(path, shortPath))
							.min(t -> t._1);
		}
		
		private static Tuple<Double,Tuple<Long,Long>>
		calcSplitDistance(List<NodeTrack> split1, List<NodeTrack> split2) {
			double total = 0;
			double minDist = Double.MAX_VALUE;
			Tuple<Long,Long> tsPair = null;
			
			for ( Tuple<NodeTrack,NodeTrack> pair: FStream.from(split1).zipWith(FStream.from(split2)) ) {
				double dist = pair._1.getLocation().distance(pair._2.getLocation());
				total += dist;
				if ( dist < minDist ) {
					minDist = dist;
					tsPair = Tuple.of(pair._1.getTimestamp(), pair._2.getTimestamp());
				}
			}
			
			return Tuple.of(total / split1.size(), tsPair);
		}
	}
}
