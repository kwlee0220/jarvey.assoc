package jarvey.assoc.feature;


import java.util.Properties;
import java.util.Set;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

import utils.func.Either;
import utils.jdbc.JdbcProcessor;

import jarvey.assoc.AssociationCollection;
import jarvey.assoc.AssociationStore;
import jarvey.assoc.FinalAssociationSelector;
import jarvey.assoc.MCMOTConfig;
import jarvey.streams.BinaryAssociationCollection;
import jarvey.streams.model.BinaryAssociation;
import jarvey.streams.model.TrackletDeleted;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.node.TrackFeature;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class FeatureAssociationStreamBuilder {
	private final MCMOTConfig m_configs;
	
	private final BinaryAssociationCollection m_binaryAssociations = new BinaryAssociationCollection(true);
	private final AssociationCollection m_associations;
	private final Set<TrackletId> m_closedTracklets;
	private final AssociationCollection m_finalAssociations;
	private final JdbcProcessor m_jdbc;
	private final Properties m_consumerProps;
	
	public FeatureAssociationStreamBuilder(MCMOTConfig configs, AssociationCollection associations,
											Set<TrackletId> closedTracklets,
											AssociationCollection finalAssociations) {
		m_configs = configs;
		m_jdbc = configs.getJdbcProcessor();
		m_consumerProps = configs.getKafkaParameters().toConsumerProperties();
		m_associations = associations;
		m_closedTracklets = closedTracklets;
		m_finalAssociations = finalAssociations;
	}
	
	public void build(KStream<String,NodeTrack> nodeTracks, KStream<String,TrackFeature> trackFeatures) {
		FeatureBinaryTrackletAssociator binaryAssociator
			= new FeatureBinaryTrackletAssociator(m_configs.getMCMOTNetwork(), m_jdbc, m_consumerProps,
													m_configs.getTopPercent(), m_binaryAssociations);

		@SuppressWarnings({ "unchecked", "deprecation" })
		KStream<String,Either<BinaryAssociation, TrackletDeleted>>[] branches =
			trackFeatures
				.filter((nodeId, tfeat) -> m_configs.getListeningNodes().contains(nodeId))
				.flatMapValues(binaryAssociator, Named.as("binary-feature-association"))
				.branch(this::isBinaryAssociation, this::isTrackletDeleted);

		// BinaryAssociation 이벤트 처리.
		branches[0]
			.mapValues(either -> either.getLeft())
			.filter((nodeId,ba) -> ba.getScore() >= m_configs.getMinBinaryAssociationScore())
			.flatMapValues(ba -> m_associations.add(ba), Named.as("feature-closure-builder"));
		
		// TrackletDeleted 이벤트 처리
		FinalAssociationSelector selector
				= new FinalAssociationSelector(m_binaryAssociations, m_associations, m_closedTracklets);
		AssociationStore assocStore = new AssociationStore(m_jdbc);
		branches[1]
			.mapValues(v -> v.getRight())
			.flatMapValues(selector, Named.as("final-feature-association"))
			.map(assocStore, Named.as("store-feature-association"))
			.flatMapValues(cl -> m_finalAssociations.add(cl));
	}
	
	private boolean isBinaryAssociation(String nodeId, Either<BinaryAssociation, TrackletDeleted> either) {
		return either.isLeft();
	}
	private boolean isTrackletDeleted(String nodeId, Either<BinaryAssociation, TrackletDeleted> either) {
		return either.isRight();
	}
}
