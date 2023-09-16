package jarvey.assoc.feature;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.compress.utils.Lists;
import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.Maps;

import utils.UnitUtils;
import utils.func.Funcs;

import jarvey.streams.model.Range;

/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class MCMOTNetwork {
	private final Map<String,ListeningNode> m_listeningNodes = Maps.newHashMap();
	
	private MCMOTNetwork() {
	}
	
	public Set<String> getListeningNodeAll() {
		return m_listeningNodes.keySet();
	}
	
	public ListeningNode getListeningNode(String nodeId) {
		return m_listeningNodes.get(nodeId);
	}
	
	private void addListeningNode(ListeningNode node) {
		m_listeningNodes.put(node.m_nodeId, node);
	}
	
	public static class ListeningNode {
		private final String m_nodeId;
		private final Map<String,List<IncomingLink>> m_incomingLinks = Maps.newHashMap();
		
		private ListeningNode(String nodeId) {
			m_nodeId = nodeId;
		}
		
		public String getNodeId() {
			return m_nodeId;
		}
		
		public List<IncomingLink> getIncomingLinks(String enterZone) {
			return m_incomingLinks.getOrDefault(enterZone, Collections.emptyList());
		}
		
		@Override
		public String toString() {
			return String.format("%s: %s", m_nodeId, m_incomingLinks);
		}
		
		private void addIncomingLink(String enterZone, String exitNode, String exitZone,
										Range<Duration> transTime) {
			IncomingLink link = new IncomingLink(exitNode, exitZone, transTime);
			List<IncomingLink> links = m_incomingLinks.computeIfAbsent(enterZone, k -> Lists.newArrayList());
			links.add(link);
		}
	}
	
	public static class IncomingLink {
		private final String m_exitNode;
		private final String m_exitZone;
		private final Range<Duration> m_transitionTimeRange;
		
		IncomingLink(String exitNode, String exitZone, Range<Duration> transTimeRange) {
			m_exitNode = exitNode;
			m_exitZone = exitZone;
			m_transitionTimeRange = transTimeRange;
		}
		
		public String getExitNode() {
			return m_exitNode;
		}
		
		public String getExitZone() {
			return m_exitZone;
		}
		
		public Range<Duration> getTransitionTimeRange() {
			return m_transitionTimeRange;
		}
		
		@Override
		public String toString() {
			return String.format("%s: ->%s %s", m_exitNode, m_exitZone, m_transitionTimeRange);
		}
	}
	
	public static MCMOTNetwork load(File yamlFile) throws IOException {
		MCMOTNetwork network = new MCMOTNetwork();
		
		Map<String, Map<String,Object>> listeningNodeDescs = new Yaml().load(new FileReader(yamlFile));
		for ( Map.Entry<String, Map<String,Object>> ent: listeningNodeDescs.entrySet() ) {
			ListeningNode listener = new ListeningNode(ent.getKey());
			for ( Map.Entry<String, Object> linkDesc: ent.getValue().entrySet() ) {
				String enterZone = linkDesc.getKey();
				
				@SuppressWarnings("unchecked")
				Map<String,Object> incomingLink = (Map<String,Object>)linkDesc.getValue();
				String exitNode = (String)incomingLink.get("incoming_node");
				String exitZone = (String)incomingLink.get("exit_zone");
				
				List<String> transTime = (List<String>)incomingLink.get("transition_time");
				List<Duration> durList = Funcs.map(transTime,
													s -> Duration.ofMillis(UnitUtils.parseDurationMillis(s)));
				Range<Duration> transTimeRange = Range.between(durList.get(0), durList.get(1));
				listener.addIncomingLink(enterZone, exitNode, exitZone, transTimeRange);
			}
			network.addListeningNode(listener);
		}
		
		return network;
	}
}