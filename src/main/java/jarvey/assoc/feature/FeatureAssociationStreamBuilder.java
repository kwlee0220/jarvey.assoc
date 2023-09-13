package jarvey.assoc.feature;


import java.util.Properties;
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
import jarvey.assoc.OverlapAreaRegistry;
import jarvey.streams.model.BinaryAssociation;
import jarvey.streams.model.BinaryAssociationCollection;
import jarvey.streams.model.JarveySerdes;
import jarvey.streams.model.TrackletDeleted;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.node.TrackFeature;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class FeatureAssociationStreamBuilder {
	private final FeatureAssociationParams m_params;
	
	private final OverlapAreaRegistry m_areaRegistry;
	private final BinaryAssociationCollection m_binaryAssociations = new BinaryAssociationCollection(true);
	private final AssociationCollection m_associations;
	private final Set<TrackletId> m_closedTracklets;
	private final AssociationCollection m_finalAssociations;
	private final JdbcProcessor m_jdbc;
	private final Properties m_consumerProps;
	private boolean m_generateGlobalTracks = true;
	
	public FeatureAssociationStreamBuilder(FeatureAssociationParams params, JdbcProcessor jdbc,
											Properties consumerProps,
											AssociationCollection associations,
											Set<TrackletId> closedTracklets,
											AssociationCollection finalAssociations) {
		m_params = params;
		m_areaRegistry = m_params.getOverlapAreaRegistry();
		m_jdbc = jdbc;;
		m_associations = associations;
		m_closedTracklets = closedTracklets;
		m_finalAssociations = finalAssociations;
		m_consumerProps = consumerProps;
	}
	
	public void setGenerateGlobalTracks(boolean flag) {
		m_generateGlobalTracks = flag;
	}
	
	public void build(KStream<String,NodeTrack> nodeTracks, KStream<String,TrackFeature> trackFeatures) {
		FeatureBinaryTrackletAssociator binaryAssociator
			= new FeatureBinaryTrackletAssociator(m_jdbc, m_consumerProps, m_params.getTopPercent(),
													m_binaryAssociations);

		@SuppressWarnings({ "unchecked", "deprecation" })
		KStream<String,Either<BinaryAssociation, TrackletDeleted>>[] branches =
			trackFeatures
				.filter((nodeId, tfeat) -> m_params.getListeningNodes().contains(nodeId))
				.flatMapValues(binaryAssociator, Named.as("binary-feature-association"))
				.branch(this::isBinaryAssociation, this::isTrackletDeleted);

		// BinaryAssociation 이벤트 처리.
		AssociationClosureBuilder assocBuilder = new AssociationClosureBuilder(m_associations);
		branches[0]
			.mapValues(either -> either.getLeft())
			.filter((nodeId,ba) -> ba.getScore() >= m_params.getMinAssociationScore())
			.flatMapValues(assocBuilder, Named.as("feature-closure-builder"));
		
		// TrackletDeleted 이벤트 처리
		FinalFeatureAssociationSelector selector
				= new FinalFeatureAssociationSelector(m_binaryAssociations, m_associations, m_closedTracklets);
		AssociationStore assocStore = new AssociationStore(m_jdbc);
		branches[1]
			.mapValues(v -> v.getRight())
			.flatMapValues(selector, Named.as("final-feature-association"))
			.map(assocStore, Named.as("store-feature-association"))
			.flatMapValues(cl -> m_finalAssociations.add(cl));
		
		if ( m_generateGlobalTracks ) {
			FeatureGlobalTrackGenerator gtrackGen
					= new FeatureGlobalTrackGenerator(m_areaRegistry, m_params.getListeningNodes(),
														m_associations);
			nodeTracks
				.flatMap(gtrackGen, Named.as("generate-feature-global-track"))
//				.print(Printed.toSysOut());
				.to(m_params.getGlobalTracksTopic(),
					Produced.with(Serdes.String(), JarveySerdes.GlobalTrack())
							.withName("to-global-tracks-tentative"));
		}
	}
	
	private boolean isBinaryAssociation(String nodeId, Either<BinaryAssociation, TrackletDeleted> either) {
		return either.isLeft();
	}
	private boolean isTrackletDeleted(String nodeId, Either<BinaryAssociation, TrackletDeleted> either) {
		return either.isRight();
	}
}
