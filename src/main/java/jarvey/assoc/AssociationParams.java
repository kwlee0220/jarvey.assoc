package jarvey.assoc;


import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Set;

import utils.CSV;
import utils.UnitUtils;

import jarvey.streams.model.BinaryAssociationCollection;

import picocli.CommandLine.Option;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class AssociationParams {
	private static final String TOPIC_NODE_TRACKS = "node-tracks";
	private static final String TOPIC_TRACK_FEATURES = "track-features";
	private static final String TOPIC_GLOBAL_TRACKS = "global-tracks-tentative";
	private static final String TOPIC_ASSOCIATIONS = "associations";

	private static final Duration DEFAULT_ASSOCIATION_INTERVAL = Duration.ofSeconds(1);
	private static final double DEFAULT_TRACK_DISTANCE = UnitUtils.parseLengthInMeter("5m");
	private static final double DEFAULT_MIN_SCORE = 0.3;
	private static final double DEFAULT_TOP_PERCENT = 0.2;

	private Set<String> m_listeningNodes;

	@Option(names={"--overlap-area"}, paramLabel="overlap-area-descriptor",
			description="overlap area description file path.")
	private String m_overlapAreaFilePath = "overlap_areas.yaml";
	private OverlapAreaRegistry m_areaRegistry = null;

	@Option(names={"--node-tracks"}, paramLabel="topic-name", description="input 'node-tracks' topic name")
	String m_nodeTracksTopic = TOPIC_NODE_TRACKS;

	@Option(names={"--track-features"}, paramLabel="topic-name", description="input 'track-features' topic name")
	String m_trackFeaturesTopic = TOPIC_TRACK_FEATURES;

	@Option(names={"--associations"}, paramLabel="topic-name",
					description="output association topic. (default: associations)")
	private String m_associationsTopic = TOPIC_ASSOCIATIONS;

	@Option(names={"--global-tracks"}, paramLabel="topic-name",
					description="output global-track topic. (default: global-tracks-overlap-tentative")
	private String m_globalTracksTopic = TOPIC_GLOBAL_TRACKS;
	
	private Duration m_assocInterval = DEFAULT_ASSOCIATION_INTERVAL;
	private double m_maxTrackDistance = DEFAULT_TRACK_DISTANCE;
	
	@Option(names={"--min-association-score"}, paramLabel="score",
			description="maximun distance difference allowance for a same track (default: 0.3).")
	private double m_minBinaryAssociationScore = DEFAULT_MIN_SCORE;
	
	@Option(names={"--top-percent"}, paramLabel="percentage",
			description="percentage for top-k score (default: 0.2).")
	private double m_topPercent = DEFAULT_TOP_PERCENT;
	
	private BinaryAssociationCollection m_binaryAssociations;
	private AssociationCollection m_associations;
	
	public String getNodeTracksTopic() {
		return m_nodeTracksTopic;
	}
	
	public String getTrackFeaturesTopic() {
		return m_trackFeaturesTopic;
	}
	
	public String getAssociationsTopic() {
		return m_associationsTopic;
	}
	
	public String getGlobalTracksTopic() {
		return m_globalTracksTopic;
	}

	public OverlapAreaRegistry getOverlapAreaRegistry() {
		if ( m_areaRegistry == null ) {
			try {
				m_areaRegistry = OverlapAreaRegistry.load(new File(m_overlapAreaFilePath));
			}
			catch ( IOException e ) {
				throw new UncheckedIOException(e);
			}
		}
		return m_areaRegistry;
	}
	
	public Set<String> getListeningNodes() {
		return m_listeningNodes;
	}
	@Option(names={"--listen"}, paramLabel="node-names", description="listening node names")
	public void setListeningNodes(String names) {
		m_listeningNodes = CSV.parseCsv(names).toSet();
	}
	
	public double getMaxTrackDistance() {
		return m_maxTrackDistance;
	}
	
	@Option(names={"--max-track-distance"}, paramLabel="distance",
			description="maximun distance difference allowance for a same track (default: 5m).")
	public void setMaxTrackDistance(String distStr) {
		m_maxTrackDistance = UnitUtils.parseLengthInMeter(distStr);
	}
	
	public Duration getAssociationInterval() {
		return m_assocInterval;
	}
	
	@Option(names={"--assoc-interval"}, paramLabel="interval",
			description="Motion-based association interval (default: 1s).")
	public void setAssociationInterval(String durationStr) {
		m_assocInterval = Duration.ofMillis(UnitUtils.parseDuration(durationStr));
	}
	
	public double getTopPercent() {
		return m_topPercent;
	}
	
	public double getMinAssociationScore() {
		return m_minBinaryAssociationScore;
	}
	
	public BinaryAssociationCollection getBinaryAssociationCollection() {
		return m_binaryAssociations;
	}
	public void setBinaryAssoicationCollection(BinaryAssociationCollection coll) {
		m_binaryAssociations = coll;
	}

	public AssociationCollection getAssociationCollection() {
		return m_associations;
	}
	public void setAssociationCollection(AssociationCollection coll) {
		m_associations = coll;
	}
}
