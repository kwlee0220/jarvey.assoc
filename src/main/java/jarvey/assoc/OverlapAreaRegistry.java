package jarvey.assoc;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.Maps;

import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class OverlapAreaRegistry {
	private final Map<String,OverlapArea> m_areas = Maps.newHashMap();
	
	public OverlapArea get(String areaId) {
		return m_areas.get(areaId);
	}
	
	public OverlapArea add(String id, OverlapArea area) {
		return m_areas.put(id, area);
	}
	
	public FOption<OverlapArea> findByNodeId(String nodeId) {
		return FStream.from(m_areas.values()).findFirst(area -> area.containsNode(nodeId));
	}
	
	public boolean containsNode(String nodeId) {
		return FStream.from(m_areas.values())
						.exists(area -> area.containsNode(nodeId));
	}
	
	public Iterable<OverlapArea> overlapAreas() {
		return FStream.from(m_areas.values());
	}
	
	public static OverlapAreaRegistry load(File yamlFile) throws IOException {
		OverlapAreaRegistry registry = new OverlapAreaRegistry();
		
		Map<String, Object> areas = new Yaml().load(new FileReader("overlap_areas.yaml"));
		for ( Map.Entry<String, Object> ent: areas.entrySet() ) {
			@SuppressWarnings("unchecked")
			OverlapArea area = OverlapArea.parse((Map<String, Object>)ent.getValue());
			registry.add(area.getId(), area);
		}
		
		return registry;
	}
}
