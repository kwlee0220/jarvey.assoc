package jarvey.assoc;

import java.util.List;
import java.util.Set;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;

import utils.Indexed;
import utils.func.Funcs;
import utils.func.Tuple;
import utils.stream.FStream;

import jarvey.streams.BinaryAssociationCollection;
import jarvey.streams.MockKeyValueStore;
import jarvey.streams.model.BinaryAssociation;
import jarvey.streams.model.TrackletId;
import jarvey.streams.serialization.json.GsonUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BinaryAssociationStore {
	private static final Logger s_logger = LoggerFactory.getLogger(BinaryAssociationStore.class);
	
	private final KeyValueStore<TrackletId,Record> m_store;
	
	public static BinaryAssociationStore fromStateStore(ProcessorContext context, String storeName) {
		KeyValueStore<TrackletId,Record> store = context.getStateStore(storeName);
		return new BinaryAssociationStore(store);
	}
	
	public static BinaryAssociationStore createLocalStore(String storeName) {
		KeyValueStore<TrackletId, Record> kvStore
			= new MockKeyValueStore<>(storeName, GsonUtils.getSerde(TrackletId.class),
										GsonUtils.getSerde(Record.class));
		return new BinaryAssociationStore(kvStore);
	}
	
	private BinaryAssociationStore(KeyValueStore<TrackletId,Record> store) {
		m_store = store;
	}
	
	public String name() {
		return m_store.name();
	}
	
	/**
	 * store에 포함된 association 객체의 갯수를 반환한다.
	 *
	 * @return	association 객체의 갯수.
	 */
	public long size() {
		return m_store.approximateNumEntries();
	}
	
	/**
	 * TrackletId로 등록된 binary association 객체들의 리스트를 반환한다.
	 * 만일 tracklet id를 포함하는 association이 없는 경우는 인자로 주어진 supplier를
	 * 호출한 결과를 반환한다.
	 * 
	 * @param trkId	검색 대상 tracklet id
	 * @return	{@link BinaryAssociation} 리스트.
	 * 			포함하는 association이 없는 경우는 supplier를 호출한 결과
	 */
	public Record getOrEmpty(TrackletId trkId) {
		return Funcs.asNonNull(m_store.get(trkId), Record::new);
	}
	
	public BinaryAssociationCollection load() {
		final List<BinaryAssociation> assocList = Lists.newArrayList();
		final Set<Set<TrackletId>> pairs = Sets.newHashSet();
		
		KeyValueIterator<TrackletId,Record> iter = m_store.all();
		while ( iter.hasNext() ) {
			Record rec = iter.next().value;
			for ( BinaryAssociation assoc: rec.association ) {
				if ( !pairs.contains(assoc.getTracklets()) ) {
					assocList.add(assoc);
					pairs.add(assoc.getTracklets());
					if ( s_logger.isDebugEnabled() ) {
						s_logger.debug("load {}", assoc);
					}
				}
			}
		}
		
		return new BinaryAssociationCollection(assocList, true);
	}
	
	public void updateAll(List<BinaryAssociation> updates) {
		final ArrayListMultimap<TrackletId,BinaryAssociation> assocListMap = ArrayListMultimap.create();
		updates.forEach(ba -> {
			assocListMap.put(ba.getLeftTrackletId(), ba);
			assocListMap.put(ba.getRightTrackletId(), ba);
		});
		
		List<KeyValue<TrackletId,Record>> storeUpdates
					= FStream.from(assocListMap.keySet())
							.map(tid -> {
								Record rec = getOrEmpty(tid);
								assocListMap.get(tid).forEach(rec::add);
								return KeyValue.pair(tid, rec);
							})
							.toList();
		m_store.putAll(storeUpdates);
	}
	
	public Record removeRecord(TrackletId trkId) {
		return m_store.delete(trkId);
	}
	
	public void removeAll(List<BinaryAssociation> updates) {
		final ListMultimap<TrackletId,BinaryAssociation> assocListMap = groupByTracklet(updates);
		
		List<KeyValue<TrackletId,Record>> storeUpdates
				= FStream.from(assocListMap.keySet())
							.map(tid -> {
								Record rec = getOrEmpty(tid);
								assocListMap.get(tid).forEach(rec::remove);
								return KeyValue.pair(tid, rec);
							})
							.toList();
		
		Tuple<List<KeyValue<TrackletId,Record>>, List<KeyValue<TrackletId,Record>>> partitions
			= Funcs.partition(storeUpdates, kv -> kv.value.size() > 0);
		FStream.from(partitions._2).map(kv -> kv.key).forEach(m_store::delete);
		m_store.putAll(partitions._1);
	}
	
	private ListMultimap<TrackletId,BinaryAssociation> groupByTracklet(List<BinaryAssociation> updates) {
		final ArrayListMultimap<TrackletId,BinaryAssociation> assocListMap = ArrayListMultimap.create();
		updates.forEach(ba -> {
			assocListMap.put(ba.getLeftTrackletId(), ba);
			assocListMap.put(ba.getRightTrackletId(), ba);
		});
		
		return assocListMap;
	}
	
	/**
	 * 주어진 tracklet id의 tracklet이 close되었음을 설정한다.
	 * 
	 * 주어진 tracklet id를 포함한 모든 binary association에 해당 id가 close됨을 설정한다.
	 * 
	 * @param trkId	'delete'된 tracklet의 식별자.
	 */
	public void markTrackletClosed(TrackletId trkId) {
		Record record = m_store.get(trkId);
		if ( record == null ) {
			// 지금까지 한번도 association이 생성되지 않았던 track인 경우
			// 'delete' 이벤트도 무시한다.
			return;
		}
		
		record.isClosed = true;
		m_store.put(trkId, record);
	}
	
	public final class Record {
		@SerializedName("is_closed") public boolean isClosed;
		@SerializedName("associations") public List<BinaryAssociation> association;
		
		public Record() {
			isClosed = false;
			association = Lists.newArrayList();
		}
		
		public int size() {
			return this.association.size();
		}
		
		public void add(BinaryAssociation assoc) {
			Indexed<BinaryAssociation> found = Funcs.findFirstIndexed(this.association, assoc::match);
			if ( found != null ) {
				this.association.set(found.index(), assoc);
			}
			else {
				this.association.add(assoc);
			}
		}
		
		public BinaryAssociation remove(BinaryAssociation assoc) {
			return Funcs.removeFirstIf(this.association, assoc::match);
		}
		
		@Override
		public String toString() {
			String closedStr = this.isClosed ? "(C) " : "";
			return String.format("%s%s", closedStr, association);
		}
	};
}
