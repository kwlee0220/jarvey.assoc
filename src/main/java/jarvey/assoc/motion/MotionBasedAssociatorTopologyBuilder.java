package jarvey.assoc.motion;


import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import jarvey.assoc.BinaryAssociation;
import jarvey.assoc.OverlapAreaRegistry;
import jarvey.streams.HoppingWindowManager;
import jarvey.streams.TrackTimestampExtractor;
import jarvey.streams.model.GlobalTrack;
import jarvey.streams.model.NodeTrack;
import jarvey.streams.model.TrackletId;
import jarvey.streams.serialization.json.GsonUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
final class MotionBasedAssociatorTopologyBuilder {
	private static final String TOPIC_OVERLAP_AREAS = "overlap-areas";
	private static final String TOPIC_GLOBAL_TRACKS = "global-tracks";
	private static final String TOPIC_MOTION_ASSOCIATIONS = "motion-associations";
	
	private static final String STORE_MOTION_ASSOCIATION = "motion-associations";
	
	private static final AutoOffsetReset DEFAULT_OFFSET_RESET = AutoOffsetReset.LATEST;
	private static final Duration WINDOW_SIZE = Duration.ofSeconds(1);
	private static final TrackTimestampExtractor TS_EXTRACTOR = new TrackTimestampExtractor();
	private static final double DEFAULT_TRACK_DISTANCE = 5;
	
	private final OverlapAreaRegistry m_areas;
	private AutoOffsetReset m_offsetReset = DEFAULT_OFFSET_RESET;
	private Duration m_windowSize = WINDOW_SIZE;
	private double m_trackDistance = DEFAULT_TRACK_DISTANCE;
	
	private AssociationClosureBuilder m_assocBuilder;
	
	MotionBasedAssociatorTopologyBuilder(OverlapAreaRegistry areas) {
		m_areas = areas;
	}
	
	public MotionBasedAssociatorTopologyBuilder withOffsetReset(AutoOffsetReset offsetReset) {
		m_offsetReset = offsetReset;
		return this;
	}
	
	public MotionBasedAssociatorTopologyBuilder withinTrackDistance(double threshold) {
		m_trackDistance = threshold;
		return this;
	}
	
	@SuppressWarnings("deprecation")
	public Topology build() {
		StreamsBuilder builder = new StreamsBuilder();
		
		builder.addStateStore(Stores.keyValueStoreBuilder(
											Stores.persistentKeyValueStore(STORE_MOTION_ASSOCIATION),
											GsonUtils.getSerde(TrackletId.class),
											GsonUtils.getListSerde(BinaryAssociation.class)));
		
		KStream<String,NodeTrack> repartitioned = builder
			// 'node-tracks' topic에 node track event들을 읽는다.
			.stream(TOPIC_OVERLAP_AREAS,
					Consumed.with(Serdes.String(), GsonUtils.getSerde(NodeTrack.class))
							.withName("source-overlap-areas")
							.withTimestampExtractor(TS_EXTRACTOR)
							.withOffsetResetPolicy(m_offsetReset));

		HoppingWindowManager windowMgr = HoppingWindowManager.ofWindowSize(m_windowSize);
		ChopNodeTracks chopper = new ChopNodeTracks(windowMgr);
		DropTooFarTracks filterFarTracks = new DropTooFarTracks(m_areas);
		
		repartitioned
			.flatMapValues(chopper, Named.as("chop-and-group-node-tracks"))
			.mapValues(filterFarTracks, Named.as("drop-far-tracks"))
			.flatTransformValues(this::createAssociationBuilder, Named.as("motion-association-builder"))
			.mapValues(AssociationClosure::toDao)
			.to(TOPIC_MOTION_ASSOCIATIONS,
					Produced.with(Serdes.String(), GsonUtils.getSerde(AssociationClosure.DAO.class))
							.withName("sink-motion-associations"));
		
		repartitioned
			.flatMapValues(createGlobalTrackGenerator(), Named.as("global-track-generator"))
			.to(TOPIC_GLOBAL_TRACKS,
					Produced.with(Serdes.String(), GsonUtils.getSerde(GlobalTrack.class))
							.withName("sink-global-tracks"));
		
		return builder.build();
	}
	
	private AssociationClosureBuilder createAssociationBuilder() {
		if ( m_assocBuilder == null ) {
			m_assocBuilder = new AssociationClosureBuilder(m_areas, m_trackDistance);
		}
		
		return m_assocBuilder;
	}
	
	private GlobalTrackGenerator createGlobalTrackGenerator() {
		return new GlobalTrackGenerator(m_areas, createAssociationBuilder().getClosureCollections());
	}
}
