package jarvey.assoc.motion;


import java.util.Set;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import utils.func.Either;
import utils.jdbc.JdbcProcessor;

import jarvey.assoc.AssociationClosureBuilder;
import jarvey.assoc.AssociationCollection;
import jarvey.assoc.AssociationStore;
import jarvey.assoc.OverlapArea;
import jarvey.assoc.OverlapAreaRegistry;
import jarvey.streams.model.BinaryAssociation;
import jarvey.streams.model.BinaryAssociationCollection;
import jarvey.streams.model.JarveySerdes;
import jarvey.streams.model.TrackletDeleted;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrack;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class MotionAssociationStreamBuilder {
	private final MotionAssociationParams m_params;
	
	private final OverlapAreaRegistry m_areaRegistry;
	private final BinaryAssociationCollection m_binaryAssociations = new BinaryAssociationCollection(true);
	private final AssociationCollection m_associations;
	private final Set<TrackletId> m_closedTracklets;
	private final AssociationCollection m_finalAssociations;
	private final JdbcProcessor m_jdbc;
	private boolean m_generateGlobalTracks = true;
	
	public MotionAssociationStreamBuilder(MotionAssociationParams params, JdbcProcessor jdbc,
											AssociationCollection associations,
											Set<TrackletId> closedTracklets,
											AssociationCollection finalAssociations) {
		m_params = params;
		m_areaRegistry = m_params.getOverlapAreaRegistry();
		m_jdbc = jdbc;
		m_associations = associations;
		m_closedTracklets = closedTracklets;
		m_finalAssociations = finalAssociations;
	}
	
	public void setGenerateGlobalTracks(boolean flag) {
		m_generateGlobalTracks = flag;
	}
	
	public void build(KStream<String,NodeTrack> nodeTracks) {
		KStream<String,NodeTrack> validNodeTracks =
			nodeTracks
				// 카메라로부터 일정 거리 이내의 track 정보만 활용한다.
				// 키 값도 node-id에서 overlap-area id로 변경시킨다.
				.filter(this::withAreaDistance)
				// key를 node id에서 overlap-area의 식별자로 변환시킨다.
				.selectKey((nodeId, ntrack) -> m_areaRegistry.findByNodeId(nodeId).getId());
			
		MotionBinaryTrackletAssociator binaryAssociator
			= new MotionBinaryTrackletAssociator(m_params.getAssociationInterval(),
												m_params.getMaxTrackDistance(), m_binaryAssociations);
		@SuppressWarnings({ "unchecked", "deprecation" })
		KStream<String,Either<BinaryAssociation, TrackletDeleted>>[] branches = 
			validNodeTracks
				// 일정 기간만큼씩 끊어서 track들 사이의 거리를 통해 binary association을 계산한다.
				.flatMap(binaryAssociator, Named.as("motion-binary-association"))
				// 생성된 BinaryAssociation 이벤트와 TrackletDeleted 이벤트를 분리해서 처리한다.
				.branch(this::isBinaryAssociation, this::isTrackletDeleted);
		
		AssociationClosureBuilder assocBuilder = new AssociationClosureBuilder(m_associations);
		KStream<String,BinaryAssociation> binaryAssociationsStream = branches[0].mapValues(Either::getLeft);
		binaryAssociationsStream
			.flatMapValues(assocBuilder, Named.as("build-motion-closure"));

		FinalMotionAssociationSelector assocSelector
			= new FinalMotionAssociationSelector(m_binaryAssociations, m_associations, m_closedTracklets);
		AssociationStore assocStore = new AssociationStore(m_jdbc);
		KStream<String,TrackletDeleted> deletedStreams = branches[1].mapValues(Either::getRight);
		deletedStreams
			.flatMapValues(assocSelector, Named.as("motion-final-association"))
			.map(assocStore, Named.as("store-motion-association"))
			.flatMapValues(cl -> m_finalAssociations.add(cl));

		if ( m_generateGlobalTracks ) {
			MotionGlobalTrackGenerator gtrackGen = new MotionGlobalTrackGenerator(m_areaRegistry, m_associations);
			validNodeTracks
				.flatMap(gtrackGen, Named.as("generate-motion-global-track"))
				.to(m_params.getGlobalTracksTopic(),
					Produced.with(Serdes.String(), JarveySerdes.GlobalTrack())
							.withName("to-global-tracks-tentative"));
		}
	}
	
	private boolean withAreaDistance(String nodeId, NodeTrack track) {
		// Overlap area에서 검출된 NodeTrack이 아니면 무시한다.
		OverlapArea area = m_areaRegistry.findByNodeId(nodeId);
		if ( area == null ) {
			return false;
		}
		
		if ( track.isDeleted() ) {
			return true;
		}
		
		double threshold = area.getDistanceThreshold(track.getNodeId());
		return track.getDistance() <= threshold;
	}
	
	private boolean isBinaryAssociation(String nodeId, Either<BinaryAssociation, TrackletDeleted> either) {
		return either.isLeft();
	}
	private boolean isTrackletDeleted(String nodeId, Either<BinaryAssociation, TrackletDeleted> either) {
		return either.isRight();
	}
}
