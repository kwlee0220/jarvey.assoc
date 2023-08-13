package jarvey.assoc.motion;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;

import jarvey.assoc.BinaryAssociation;
import jarvey.streams.MockKeyValueStore;
import jarvey.streams.model.TrackletId;
import jarvey.streams.serialization.json.GsonUtils;
import utils.Indexed;
import utils.func.Funcs;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class BinaryAssociationStore {
	private static final Logger s_logger = LoggerFactory.getLogger(BinaryAssociationStore.class);
	
	private final KeyValueStore<TrackletId,Record> m_store;
	private final boolean m_keepBestAssociationOnly;
	
	public static BinaryAssociationStore fromStateStore(ProcessorContext context, String storeName) {
		KeyValueStore<TrackletId,Record> store = context.getStateStore(storeName);
		return new BinaryAssociationStore(store, false);
	}
	
	public static BinaryAssociationStore createLocalStore(String storeName) {
		KeyValueStore<TrackletId,Record> kvStore
			= new MockKeyValueStore<>(storeName, GsonUtils.getSerde(TrackletId.class),
										GsonUtils.getSerde(Record.class));
		return new BinaryAssociationStore(kvStore, false);
	}
	
	private BinaryAssociationStore(KeyValueStore<TrackletId,Record> store, boolean keepBestAssociationOly) {
		m_keepBestAssociationOnly = keepBestAssociationOly;
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
	 * 
	 * @param trkId	검색 대상 tracklet id
	 * @return	{@link BinaryAssociation} 리스트.
	 * 			포함하는 association이 없는 경우는 null이 반환됨.
	 */
	public Record get(TrackletId trkId) {
		return m_store.get(trkId);
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
		return Funcs.asNonNull(get(trkId), Record::new);
	}
	
	public Iterator<KeyValue<TrackletId,Record>> all() {
		return m_store.all();
	}
	
	public void put(TrackletId trkId, Record rec) {
		if ( rec.association.isEmpty() ) {
			m_store.delete(trkId);
		}
		else {
			m_store.put(trkId, rec);
		}
	}
	
	/**
	 * 주어진 binary assocation을 등록한다.
	 * 
	 * @param assoc	등록시킬 {@link BinaryAssociation} 객체.
	 * @return	갱신 여부. 등록시킬 association의 점수에 따라 등록이 무시될 수 있고,
	 * 			무시되어 반영되지 않은 경우는 {@code false} 그렇지 않은 경우는 {@code true}.
	 */
	public boolean add(BinaryAssociation assoc) {
		final TrackletId leftTrkId = assoc.getLeftTrackId();
		final TrackletId rightTrkId = assoc.getRightTrackId();
		
		Record leftRec = getOrEmpty(leftTrkId);
		Record rightRec = getOrEmpty(rightTrkId);
		
		// 오른쪽 tracklet id로 등록된 binary assocation 세트에서 'assoc'와 동일 구조의
		// 것이 있는가 검색한다.
		Indexed<BinaryAssociation> found = Funcs.findFirstIndexed(rightRec.association, ba -> ba.match(assoc));
		
		// 동일 track들로 구성된 binary association이 이미 존재하는 경우에는
		// 그것과의 score를 비교해여 갱신 여부를 결정한다.
		if ( found != null ) {
			if ( found.value().getScore() < assoc.getScore() ) {
				// 새로 삽입될 assoc의 score가 더 높은 경우는 기존을 대신해서 새 association을 대체하고,
				// 왼쪽 tracklet id로 등록된 binary association도 대체시킨다.
				rightRec.association.set(found.index(), assoc);
				
				Funcs.replaceFirst(leftRec.association, assoc::match, assoc);
				
				// 갱신된 두 binary association 세트를 store에 저장한다.
				m_store.putAll(Arrays.asList(KeyValue.pair(leftTrkId, leftRec),
											KeyValue.pair(rightTrkId, rightRec)));
				if ( s_logger.isDebugEnabled() ) {
					String msg = String.format("update assocation: %s-%s score=(%.2f->%.2f)",
												assoc.getLeftTrackId(), assoc.getRightTrackId(),
												found.value().getScore(), assoc.getScore());
					s_logger.debug(msg);
				}
				return true;
			}
			else {
				if ( s_logger.isDebugEnabled() ) {
					String msg = String.format("drop low-score assocation: %s-%s (%.2f<%.2f)",
												assoc.getLeftTrackId(), assoc.getRightTrackId(),
												assoc.getScore(), found.value().getScore());
					s_logger.debug(msg);
				}
				return false;
			}
		}
		
		//
		// 동일 binary association이 존재하지 않는 경우.
		//
		
		if ( m_keepBestAssociationOnly ) {
			List<BinaryAssociation> removeds1, removeds2;
			
			// 추가할 association과 conflict를 유발하는 기등록된 binary association들 중에서
			// score가 더 높은 것이 있는지 확인한다.
			if ( Funcs.exists(leftRec.association,
							ba -> ba.containsNode(rightTrkId.getNodeId()) && ba.getScore() > assoc.getScore()) ) {
				return false;
			}
			if ( Funcs.exists(rightRec.association,
							ba -> ba.containsNode(leftTrkId.getNodeId()) && ba.getScore() > assoc.getScore()) ) {
				return false;
			}

			// 기존에 등록된 binary association들 중에서 conflict를 유발하며 score가 낮은 경우 이를 제거한다.
			removeds1 = Funcs.removeIf(leftRec.association,
							ba -> ba.containsNode(rightTrkId.getNodeId()) && ba.getScore() < assoc.getScore());
			removeds2 = Funcs.removeIf(rightRec.association,
							ba -> ba.containsNode(leftTrkId.getNodeId()) && ba.getScore() < assoc.getScore());
			List<BinaryAssociation> mergeds = Funcs.union(removeds1, removeds2);
			Set<BinaryAssociation> removeds = Sets.newHashSet(mergeds);
			if ( removeds.size() > 0 && s_logger.isDebugEnabled() ) {
				s_logger.debug("remove conflicting associations: {} < {}", removeds, assoc);
			}
		}
		
		// Binary association을 구성하는 두 tracklet id로 등록된 association 세트를 변경시킨다.
		leftRec.association.add(assoc);
		rightRec.association.add(assoc);
		m_store.putAll(Arrays.asList(KeyValue.pair(assoc.getLeftTrackId(), leftRec),
									KeyValue.pair(assoc.getRightTrackId(), rightRec)));
		
		return true;
	}
	
	/**
	 * 주어진 tracklet id의 tracklet이 close되었음을 설정한다.
	 * 
	 * 주어진 tracklet id를 포함한 모든 binary association에 해당 id가 close됨을 설정한다.
	 * 
	 * @param trkId	'delete'된 tracklet의 식별자.
	 */
	public void markTrackletClosed(TrackletId trkId) {
		Record record = get(trkId);
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
		
		@Override
		public String toString() {
			String closedStr = this.isClosed ? "(C) " : "";
			return String.format("%s%s", closedStr, association);
		}
	};
}
