package jarvey.assoc.motion;

import java.util.List;
import java.util.Map;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import jarvey.streams.MockKeyValueStore;
import jarvey.streams.serialization.json.GsonUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AssociationClosureStore {
	private static final Logger s_logger = LoggerFactory.getLogger(AssociationClosureStore.class);
	
	private final KeyValueStore<String,Record> m_kvStore;
	
	public static AssociationClosureStore fromStateStore(ProcessorContext context, String storeName) {
		KeyValueStore<String,Record> store = context.getStateStore(storeName);
		return new AssociationClosureStore(store);
	}
	
	public static AssociationClosureStore createLocalStore(String storeName) {
		KeyValueStore<String,Record> kvStore
			= new MockKeyValueStore<>(storeName, GsonUtils.getSerde(String.class),
										GsonUtils.getSerde(Record.class));
		return new AssociationClosureStore(kvStore);
	}
	
	private AssociationClosureStore(KeyValueStore<String,Record> store) {
		m_kvStore = store;
	}
	
	public String name() {
		return m_kvStore.name();
	}
	
	public Map<String,AssociationCollection<AssociationClosure>> buildCache(boolean keepBestAssociationOnly) {
		Map<String,AssociationCollection<AssociationClosure>> cache = Maps.newHashMap();
		
		KeyValueIterator<String, Record> iter = m_kvStore.all();
		while ( iter.hasNext() ) {
			KeyValue<String, Record> kv = iter.next();
			AssociationCollection<AssociationClosure> coll
				= new AssociationCollection<AssociationClosure>(Lists.newArrayList(kv.value.m_closures),
																keepBestAssociationOnly);
			cache.put(kv.key, coll);
		}
		
		return cache;
	}
	
	public void load(String key, AssociationCollection<AssociationClosure> coll) {
		Record rec = m_kvStore.get(key);
		if ( rec != null ) {
			rec.m_closures.forEach(coll::add);
		}
	}
	
	public void write(String key, AssociationCollection<AssociationClosure> coll) {
		Record record = new Record(Lists.newArrayList(coll));
		m_kvStore.put(key, record);
	}
	
	private final class Record {
		@SerializedName("closures") private final List<AssociationClosure> m_closures;
		
		private Record(List<AssociationClosure> closures) {
			this.m_closures = closures;
		}
	};
}
