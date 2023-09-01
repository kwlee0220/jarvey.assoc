package jarvey.assoc.feature;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class MCMOTNetwork {
	private Map<String,Map<String,List<IncomingLink>>> m_vertice = Maps.newHashMap();
	
	public static class IncomingLink {
		private final String m_exitNode;
		private final String m_exitZone;
		private final Duration m_transitionTime;
		
		IncomingLink(String exitNode, String exitZone, Duration transTime) {
			m_exitNode = exitNode;
			m_exitZone = exitZone;
			m_transitionTime = transTime;
		}
		
		public String getExitNode() {
			return m_exitNode;
		}
		
		public String getExitZone() {
			return m_exitZone;
		}
		
		public Duration getTransitionTime() {
			return m_transitionTime;
		}
		
		@Override
		public String toString() {
			return String.format("%s: ->%s [%s]", m_exitNode, m_exitZone, m_transitionTime);
		}
	}
	
	public MCMOTNetwork() {
		Map<String,List<IncomingLink>> zoneDesc = Maps.newHashMap();
//		zoneDesc.put("A",
//					Arrays.asList(
//						new IncomingLink("etri:07", "A", Duration.ofSeconds(3))
//					));
//		m_vertice.put("etri:04", zoneDesc);
		
		zoneDesc = Maps.newHashMap();
		zoneDesc.put("A",
					Arrays.asList(
						new IncomingLink("etri:07", "A", Duration.ofSeconds(1))
					));
		m_vertice.put("etri:05", zoneDesc);

//		zoneDesc = Maps.newHashMap();
//		zoneDesc.put("A",
//					Arrays.asList(
//						new IncomingLink("etri:07", "A", Duration.ofSeconds(2))
//					));
//		m_vertice.put("etri:06", zoneDesc);

		zoneDesc = Maps.newHashMap();
		zoneDesc.put("A",
					Arrays.asList(
//						new IncomingLink("etri:04", "A", Duration.ofSeconds(3)),
						new IncomingLink("etri:05", "A", Duration.ofMillis(1500))
//						new IncomingLink("etri:06", "A", Duration.ofSeconds(2))
					));
		m_vertice.put("etri:07", zoneDesc);
	}
	
	public List<IncomingLink> getIncomingLinks(String node, String enterZone) {
		Map<String,List<IncomingLink>> vertex = m_vertice.get(node);
		if ( vertex == null ) {
			return null;
		}
		else {
			return vertex.get(enterZone);
		}
	}
}