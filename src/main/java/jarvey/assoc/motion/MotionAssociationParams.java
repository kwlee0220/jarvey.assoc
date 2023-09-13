package jarvey.assoc.motion;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;

import utils.UnitUtils;

import jarvey.assoc.OverlapAreaRegistry;

import picocli.CommandLine.Option;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class MotionAssociationParams {
	private static final String TOPIC_NODE_TRACKS = "node-tracks";
	private static final String TOPIC_GLOBAL_TRACKS = "global-tracks-tentative";
	private static final String TOPIC_ASSOCIATIONS = "associations";
	private static final Duration DEFAULT_ASSOCIATION_INTERVAL = Duration.ofSeconds(1);
	private static final double DEFAULT_TRACK_DISTANCE = UnitUtils.parseLengthInMeter("5m");

	@Option(names={"--node-tracks"}, paramLabel="topic-name", description="input 'node-tracks' topic name")
	private String m_nodeTracksTopic = TOPIC_NODE_TRACKS;

	@Option(names={"--associations"}, paramLabel="topic-name",
					description="output association topic. (default: associations)")
	private String m_associationsTopic = TOPIC_ASSOCIATIONS;

	@Option(names={"--global-tracks"}, paramLabel="topic-name",
					description="output global-track topic. (default: global-tracks-overlap-tentative")
	private String m_globalTracksTopic = TOPIC_GLOBAL_TRACKS;

	@Option(names={"--overlap-area"}, paramLabel="overlap-area-descriptor",
			description="overlap area description file path.")
	private String m_overlapAreaFilePath = "overlap_areas.yaml";
	private OverlapAreaRegistry m_areaRegistry = null;
	
	private Duration m_assocInterval = DEFAULT_ASSOCIATION_INTERVAL;
	private double m_maxTrackDistance = DEFAULT_TRACK_DISTANCE;
	
	public String getNodeTracksTopic() {
		return m_nodeTracksTopic;
	}
	
	public void setNodeTracksTopic(String name) {
		m_nodeTracksTopic = name;
	}
	
	public String getAssociationsTopic() {
		return m_associationsTopic;
	}
	
	public void setAssociationsTopic(String name) {
		m_associationsTopic = name;
	}
	
	public String getGlobalTracksTopic() {
		return m_globalTracksTopic;
	}
	
	public void setGlobalTracksTopic(String name) {
		m_globalTracksTopic = name;
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
	
	public void setOverlapRegistry(OverlapAreaRegistry areaRegistry) {
		m_areaRegistry = areaRegistry;
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
}
