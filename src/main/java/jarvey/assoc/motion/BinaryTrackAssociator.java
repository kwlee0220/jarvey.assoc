package jarvey.assoc.motion;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import utils.func.Either;
import utils.func.Funcs;
import utils.func.Tuple;
import utils.stream.FStream;

import jarvey.assoc.BinaryAssociation;
import jarvey.streams.EventCollectingWindowAggregation;
import jarvey.streams.HoppingWindowManager;
import jarvey.streams.Windowed;
import jarvey.streams.model.Timestamped;
import jarvey.streams.model.TrackletDeleted;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrack;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class BinaryTrackAssociator
		implements Transformer<String, NodeTrack,
								Iterable<KeyValue<String, Either<BinaryAssociation, TrackletDeleted>>>> {
	private static final Logger s_logger = LoggerFactory.getLogger(BinaryTrackAssociator.class);
	
	private final EventCollectingWindowAggregation<TaggedTrack> m_aggregation;
	private double m_trackDistanceThreshold;
	private final Map<TrackletId,TrackletDeleted> m_closedTracklets = Maps.newHashMap();

	private AssociationCollection<BinaryAssociation> m_collection;
	private final String m_storeName;
	private BinaryAssociationStore m_store;
	private final boolean m_useMockStore;
	
	public BinaryTrackAssociator(Duration splitInterval, double trackDistanceThreshold,
								String storeName, boolean useMockStore) {
		HoppingWindowManager windowMgr = HoppingWindowManager.ofWindowSize(splitInterval);
		m_aggregation = new EventCollectingWindowAggregation<>(windowMgr);
		
		m_trackDistanceThreshold = trackDistanceThreshold;
		m_collection = new AssociationCollection<>(false);
		
		m_storeName = storeName;
		if ( useMockStore ) {
			m_store = BinaryAssociationStore.createLocalStore(storeName);
		}
		m_useMockStore = useMockStore;
	}

	@Override
	public void close() {
	}

	@Override
	public void init(ProcessorContext context) {
		if ( !m_useMockStore ) {
			m_store = BinaryAssociationStore.fromStateStore(context, m_storeName);
		}
		m_collection = m_store.load();
	}
	
	public Set<TrackletId> getClosedTracklets() {
		return m_closedTracklets.keySet();
	}

	@Override
	public Iterable<KeyValue<String, Either<BinaryAssociation, TrackletDeleted>>>
	transform(String areaId, NodeTrack track) {
		List<KeyValue<String, Either<BinaryAssociation, TrackletDeleted>>> results = Lists.newArrayList();
		
		List<Windowed<List<TaggedTrack>>> windoweds = m_aggregation.collect(new TaggedTrack(areaId, track));
		for ( KeyValue<String,List<NodeTrack>> keyedBucket: FStream.from(windoweds)
																	.flatMapIterable(this::groupByArea) ) {
			String bucketAreaId = keyedBucket.key;
			List<NodeTrack> bucket = keyedBucket.value;
			
			List<Either<BinaryAssociation, TrackletDeleted>> output
				= FStream.from(associate(bucket))
						.flatMapNullable(this::updateStore)
						.sort(either -> {
							if ( either.isLeft() ) {
								return either.getLeft().getTimestamp();
							}
							else {
								return either.getRight().getTimestamp();
							}
						})
						.toList();
			if ( output.size() > 0 ) {
				// 변경되거나 추가된 binary association을 store에 반영시킨다.
				List<BinaryAssociation> newAssocList = FStream.from(output)
																.flatMapOption(either -> either.left())
																.toList();
				m_store.updateAll(newAssocList);
				
				output.forEach(either -> results.add(KeyValue.pair(bucketAreaId, either)));
			}
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
						outEvents.add(Either.left(assoc));
					}
				}
			}
		}
		
		// delete 이벤트는 마지막으로 추가한다.
		deleteds.forEach((trkId, deleted) -> {
			m_closedTracklets.put(trkId, deleted);
			m_store.markTrackletClosed(trkId);
			
			outEvents.add(Either.right(deleted));
		});
		
		return outEvents;
	}
	
	public void purgeClosedBinaryAssociation(Set<TrackletId> trkIds) {
		// 주어진 tracklet이 포함된 모든 binary association을 제거한다.
		List<BinaryAssociation> purgeds = Funcs.removeIf(m_collection,
														ba -> Funcs.intersects(ba.getTracklets(), trkIds));
		if ( s_logger.isDebugEnabled() ) {
			purgeds.forEach(ba -> s_logger.debug("delete binary-association: {}", ba));
		}
		trkIds.forEach(m_store::removeRecord);
		
		trkIds.forEach(m_closedTracklets::remove);
		if ( s_logger.isDebugEnabled() ) {
			trkIds.forEach(trkId -> s_logger.debug("delete tracklet: {}", trkId));
		}
	}
	
	public List<TrackletDeleted> removeDanglingClosedTracklets() {
		// close된 tracklet들 중에서 binary association이 없는 경우
		// 해당 tracklet은 다른 tracklet과 association 없이 종료된 것으로
		// 단일 association으로 구성된 closure를 생성한다.
		List<utils.func.KeyValue<TrackletId,TrackletDeleted>> danglings
				= Funcs.removeIf(m_closedTracklets,
								(tid, ev) -> !Funcs.exists(m_collection, ba -> ba.containsTracklet(tid)));
		if ( s_logger.isDebugEnabled() && danglings.size() > 0 ) {
			danglings.forEach(trkId -> s_logger.debug("delete dangling tracklet: {}", trkId));
		}
		
		return Funcs.map(danglings, kv -> kv.value());
	}
	
	private Either<BinaryAssociation,TrackletDeleted>
	updateStore(Either<BinaryAssociation,TrackletDeleted> ev) {
		if ( ev.isLeft() ) {
			if ( m_collection.add(ev.getLeft()) ) {
				return ev;
			}
			else {
				return null;
			}
		}
		else {
			return ev;
		}
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
				return new BinaryAssociation(leftKey, rightKey, score, ret._2._1, ret._2._2);
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
