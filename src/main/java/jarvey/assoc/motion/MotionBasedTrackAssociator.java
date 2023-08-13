package jarvey.assoc.motion;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import utils.func.Either;
import utils.func.FOption;
import utils.func.Funcs;
import utils.func.Tuple;
import utils.stream.FStream;

import jarvey.assoc.BinaryAssociation;
import jarvey.assoc.OverlapArea;
import jarvey.assoc.OverlapAreaRegistry;
import jarvey.streams.model.NodeTrack;
import jarvey.streams.model.TrackletDeleted;
import jarvey.streams.model.TrackletId;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class MotionBasedTrackAssociator
		implements ValueMapperWithKey<String, List<NodeTrack>,
										Iterable<Either<BinaryAssociation,TrackletDeleted>>> {
	private static final Logger s_logger = LoggerFactory.getLogger(MotionBasedTrackAssociator.class);
	
	private final OverlapAreaRegistry m_areaRegistry;
	private final AssociationCollection<BinaryAssociation> m_collection;
	private double m_trackDistanceThreshold;
	private final Set<TrackletId> m_closedTracklets = Sets.newHashSet();
	
	public MotionBasedTrackAssociator(OverlapAreaRegistry registry, double trackDistanceThreshold) {
		m_areaRegistry = registry;
		m_collection = new AssociationCollection<>(false);
		m_trackDistanceThreshold = trackDistanceThreshold;
	}
	
	public Set<TrackletId> getClosedTracklets() {
		return m_closedTracklets;
	}

	@Override
	public Iterable<Either<BinaryAssociation, TrackletDeleted>>
	apply(String areaId, List<NodeTrack> bucket) {
		if ( areaId == null ) {
			// overlapped area에 포함되지 않는 node에서 생성된 track의 경우
			// association될 수 없기 때문에 바로 empty list를 반환한다.
			return Collections.emptyList();
		}
		
		OverlapArea area = m_areaRegistry.get(areaId);
		if ( area == null ) {
			s_logger.warn("unexpected area id: {}", areaId);
			return Collections.emptyList();
		}
		
		return FStream.from(associate(area, bucket))
						.flatMapOption(this::updateStore)
						.sort(either -> {
							if ( either.isLeft() ) {
								return either.getLeft().getTimestamp();
							}
							else {
								return either.getRight().getTimestamp();
							}
						})
						.toList();
	}
	
	public void purgeClosedBinaryAssociation(Set<TrackletId> trkIds) {
		List<BinaryAssociation> purgeds = Funcs.removeIf(m_collection,
														ba -> Funcs.intersects(ba.getTracklets(), trkIds));
		if ( s_logger.isDebugEnabled() ) {
			purgeds.forEach(ba -> s_logger.debug("delete binary-association: {}", ba));
		}
		
		m_closedTracklets.removeAll(trkIds);
		if ( s_logger.isDebugEnabled() ) {
			trkIds.forEach(trkId -> s_logger.debug("delete tracklet: {}", trkId));
		}
		
		List<TrackletId> danglings = Funcs.removeIf(m_closedTracklets,
											tid -> !Funcs.exists(m_collection, ba -> ba.containsTracklet(tid)));
		if ( s_logger.isDebugEnabled() ) {
			danglings.forEach(trkId -> s_logger.debug("delete dangling tracklet: {}", trkId));
		}
	}
	
	private List<Either<BinaryAssociation,TrackletDeleted>>
	associate(OverlapArea area, List<NodeTrack> tracks) {
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
			m_closedTracklets.add(trkId);
			outEvents.add(Either.right(deleted));
		});
		
		return outEvents;
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
				// FIXME 나중에 정리 (return을 바로하기)
				BinaryAssociation ba = new BinaryAssociation(leftKey, rightKey, score, ret._2._1, ret._2._2);
				return ba;
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
	
	private FOption<Either<BinaryAssociation,TrackletDeleted>>
	updateStore(Either<BinaryAssociation,TrackletDeleted> ev) {
		if ( ev.isLeft() ) {
			if ( m_collection.add(ev.getLeft()) ) {
				return FOption.of(ev);
			}
			else {
				return FOption.empty();
			}
		}
		else {
			return FOption.of(ev);
		}
	}
}
