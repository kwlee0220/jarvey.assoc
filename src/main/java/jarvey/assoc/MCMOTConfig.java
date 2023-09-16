package jarvey.assoc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.Sets;

import utils.CSV;
import utils.UnitUtils;
import utils.jdbc.JdbcParameters;
import utils.jdbc.JdbcProcessor;

import jarvey.assoc.feature.MCMOTNetwork;
import jarvey.streams.KafkaParameters;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class MCMOTConfig {
	private static final Logger s_logger = LoggerFactory.getLogger(MCMOTConfig.class);
	
	private String m_nodeTracksTopic;
	private String m_trackFeaturesTopic;
	
	private String m_globalTracksTopic;
	private Duration m_outputDelay;
	
	private OverlapAreaRegistry m_areaRegistry;
	private Duration m_assocInterval;
	private Duration m_graceTime;
	private double m_maxMatchDistance;
	
	private MCMOTNetwork m_mcmotNetwork;
	private Set<String> m_listeningNodes;
	private double m_minBinaryAssociationScore;
	private double m_topPercent;
	
	private JdbcProcessor m_jdbc;
	private KafkaParameters m_kafkaParams = new KafkaParameters();
	
	@SuppressWarnings("unchecked")
	public static MCMOTConfig load(File configFile) throws FileNotFoundException {
		MCMOTConfig configs = new MCMOTConfig();

		Map<String,Object> empty = Collections.emptyMap();
		Map<String, Object> configDescs = new Yaml().load(new FileReader(configFile));

		Map<String,String> kafka = (Map<String,String>)configDescs.getOrDefault("kafka", empty);
		configs.m_kafkaParams.setApplicationId(kafka.getOrDefault("client-id", "associator"));
		configs.m_kafkaParams.setBootstrapServers(kafka.getOrDefault("bootstrap-servers", "localhost:9092"));
		configs.m_kafkaParams.setAutoOffsetReset(kafka.getOrDefault("auto-offset-reset", "latest"));
		
		Map<String,String> inputTopics = (Map<String,String>)configDescs.getOrDefault("input-topics", empty);
		configs.m_nodeTracksTopic = inputTopics.getOrDefault("node-tracks", "node-tracks");
		configs.m_trackFeaturesTopic = inputTopics.getOrDefault("track-features", "node-tracks");

		String jdbcUrl = (String)configDescs.get("jdbc");
		if ( jdbcUrl != null ) {
			JdbcParameters jdbcParams = new JdbcParameters();
			jdbcParams.jdbcLoc(jdbcUrl);
			configs.m_jdbc = jdbcParams.createJdbcProcessor();
		}

		Map<String,Object> motionConfigs = (Map<String,Object>)configDescs.getOrDefault("motion", empty);
		parseMotionConfigs(motionConfigs, configs);

		Map<String,Object> featureConfigs = (Map<String,Object>)configDescs.getOrDefault("feature", empty);
		parseFeatureConfigs(featureConfigs, configs);

		Map<String,String> outputConfigs = (Map<String,String>)configDescs.getOrDefault("output", empty);
		parseOutputConfigs(outputConfigs, configs);
		
		return configs;
	}
	
	public KafkaParameters getKafkaParameters() {
		return m_kafkaParams;
	}
	
	public String getNodeTracksTopic() {
		return m_nodeTracksTopic;
	}
	
	public String getTrackFeaturesTopic() {
		return m_trackFeaturesTopic;
	}
	
	public String getGlobalTracksTopic() {
		return m_globalTracksTopic;
	}
	
	public Duration getOutputDelay() {
		return m_outputDelay;
	}

	public OverlapAreaRegistry getOverlapAreaRegistry() {
		return m_areaRegistry;
	}
	
	public MCMOTNetwork getMCMOTNetwork() {
		return m_mcmotNetwork;
	}

	public void setMCMOTNetworkFile(String path) throws IOException {
		m_mcmotNetwork = MCMOTNetwork.load(new File(path));
		if ( m_listeningNodes == null ) {
			m_listeningNodes = m_mcmotNetwork.getListeningNodeAll();
		}
	}
	
	public Set<String> getListeningNodes() {
		return m_listeningNodes;
	}
	
	public void setListeningNodes(String names) {
		m_listeningNodes = CSV.parseCsv(names).toSet();
	}
	
	public JdbcProcessor getJdbcProcessor() {
		return m_jdbc;
	}
	
	public double getMaxTrackDistance() {
		return m_maxMatchDistance;
	}
	
	public void setMaxTrackDistance(String distStr) {
		m_maxMatchDistance = UnitUtils.parseLengthInMeter(distStr);
	}
	
	public Duration getAssociationInterval() {
		return m_assocInterval;
	}
	
	public Duration getMatchingGraceTime() {
		return m_graceTime;
	}
	
	public void setAssociationInterval(String durationStr) {
		m_assocInterval = UnitUtils.parseDuration(durationStr);
	}
	
	public double getTopPercent() {
		return m_topPercent;
	}
	
	public double getMinBinaryAssociationScore() {
		return m_minBinaryAssociationScore;
	}
	
	@SuppressWarnings("unchecked")
	private static void parseMotionConfigs(Map<String,Object> motionConfigs, MCMOTConfig configs) {
		String overlapAreaFilePath = (String)motionConfigs.getOrDefault("overlap-area", "overlap_areas.yaml");
		try {
			configs.m_areaRegistry = OverlapAreaRegistry.load(new File(overlapAreaFilePath));
		}
		catch ( IOException ignored ) { }

		String str;
		Map<String,String> matchConfigs = (Map<String,String>)motionConfigs.getOrDefault("match",
																					Collections.emptyMap());
		str = matchConfigs.getOrDefault("window-size", "1s");
		configs.m_assocInterval = UnitUtils.parseDuration(str);
		str = matchConfigs.getOrDefault("grace-time", "0");
		configs.m_graceTime = UnitUtils.parseDuration(str);
		str = matchConfigs.getOrDefault("max-distance", "5m");
		configs.m_maxMatchDistance = UnitUtils.parseLengthInMeter(str);	
	}
	
	@SuppressWarnings("unchecked")
	private static void parseFeatureConfigs(Map<String,Object> featureConfigs, MCMOTConfig configs) {
		String networkFilePath = (String)featureConfigs.getOrDefault("camera-network", "mcmot_network.yaml");
		try {
			configs.m_mcmotNetwork = MCMOTNetwork.load(new File(networkFilePath));
		}
		catch ( IOException ignored ) { }
		
		List<String> nodeList = (List<String>)featureConfigs.get("listening-nodes");
		if ( nodeList != null ) {
			configs.m_listeningNodes = Sets.newHashSet(nodeList);
			Set<String> listeningNodesDiff = Sets.difference(configs.m_listeningNodes,
															configs.m_mcmotNetwork.getListeningNodeAll());
			if ( !listeningNodesDiff.isEmpty() ) {
				s_logger.warn("Some listening nodes are not specified in the network configuration: nodes={}",
								listeningNodesDiff);
				configs.m_listeningNodes = Sets.intersection(configs.m_listeningNodes,
															configs.m_mcmotNetwork.getListeningNodeAll());
			}
		}
		else {
			configs.m_listeningNodes = configs.m_mcmotNetwork.getListeningNodeAll();
		}

		Map<String,Object> matchConfigs = (Map<String,Object>)featureConfigs.getOrDefault("match",
																					Collections.emptyMap());
		configs.m_minBinaryAssociationScore = (double)matchConfigs.getOrDefault("min-score", 0.3);
		configs.m_topPercent = (double)matchConfigs.getOrDefault("top-percent", 0.2);
	}
	
	private static void parseOutputConfigs(Map<String,String> outputConfigs, MCMOTConfig configs) {
		configs.m_globalTracksTopic = outputConfigs.getOrDefault("topic", "global-tracks-tentative");
		configs.m_outputDelay = UnitUtils.parseDuration(outputConfigs.getOrDefault("delay", "0"));
	}
}
	
