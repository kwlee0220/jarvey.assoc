package jarvey.assoc.motion;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import jarvey.assoc.BinaryAssociation;
import jarvey.streams.model.TrackletId;
import utils.Indexed;
import utils.func.Funcs;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BinaryAssociationCollection implements Iterable<BinaryAssociation>  {
	private static final Logger s_logger = LoggerFactory.getLogger(BinaryAssociationCollection.class);
	
	private final boolean m_keepBestAssociationOly;
	private final List<BinaryAssociation> m_associations = Lists.newArrayList();
	
	public BinaryAssociationCollection(boolean keepBestAssociationOnly) {
		m_keepBestAssociationOly = keepBestAssociationOnly;
	}
	
	/**
	 * Collection에 포함된 association 객체의 갯수를 반환한다.
	 *
	 * @return	association 객체의 갯수.
	 */
	public long size() {
		return m_associations.size();
	}

	/**
	 * 주어진 tracklet 들로 구성된 association 객체를 검색한다.
	 *
	 * @param key	검색 키로 사용할 tracklet 집합.
	 * @return	검색된 association 객체. 존재하지 않는 경우에는 null.
	 */
	public BinaryAssociation get(Set<TrackletId> key) {
		return Funcs.findFirst(m_associations, a -> key.equals(a.getTracklets()));
	}

	/**
	 * 주어진 tracklet 들로 구성된 association 객체와 collection 내의 순번을 반환한다.
	 *
	 * @param key	검색 키로 사용할 tracklet 집합.
	 * @return	검색된 association 및 순번 객체. 존재하지 않는 경우에는 null.
	 */
	public Indexed<BinaryAssociation> getIndexed(Set<TrackletId> key) {
		return FStream.from(m_associations)
						.zipWithIndex()
						.findFirst(t -> key.equals(t._1.getTracklets()))
						.map(t -> Indexed.with(t._1, t._2))
						.getOrNull();
	}
	
	/**
	 * 주어진 tracklet을 포함하는 모든 association을 반환한다.
	 *
	 * @param key	검색 키로 사용할 tracklet의 식별자.
	 * @return	검색된 association의 stream 객체.
	 */
	public FStream<BinaryAssociation> findAll(TrackletId key) {
		return FStream.from(m_associations)
						.filter(a -> a.containsTracklet(key));
	}
	
	public FStream<Indexed<BinaryAssociation>> findIndexedAll(TrackletId key) {
		return FStream.from(m_associations)
						.zipWithIndex()
						.map(t -> Indexed.with(t._1, t._2))
						.filter(idxed -> idxed.value().containsTracklet(key));
	}
	
	public boolean add(BinaryAssociation assoc) {
		final Set<TrackletId> trkId = assoc.getTracklets();

		boolean replaced = false;
		Indexed<BinaryAssociation> found = getIndexed(trkId);
		if ( found != null ) {
			if ( found.value().getScore() < assoc.getScore() ) {
				m_associations.set(found.index(), assoc);
				replaced = true;
			}
		}
		if ( m_keepBestAssociationOly ) {
			if ( replaced ) {
				return true;
			}
			if ( found != null ) {
				return false;
			}
			else {
				m_associations.add(assoc);
				return true;
			}
		}
		else {
			m_associations.add(assoc);
			return true;
		}
	}
	
	/**
	 * 주어진 tracklet들로 구성된 association을 collection에서 제거한다.
	 *
	 * @param key	tracklet id 집합
	 * @return	제거된 association 객체. 해당 키의 association 존재하지 않은 경우는 {@code null}.
	 */
	public BinaryAssociation remove(Set<TrackletId> key) {
		return Funcs.removeFirstIf(m_associations, a -> a.match(key));
	}
	
	public BinaryAssociation remove(int index) {
		return m_associations.remove(index);
	}

	@Override
	public Iterator<BinaryAssociation> iterator() {
		return m_associations.iterator();
	}
	
	public void clear() {
		m_associations.clear();
	}
}
