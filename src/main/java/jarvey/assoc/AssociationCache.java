package jarvey.assoc;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import utils.func.FOption;

import jarvey.streams.model.Association;
import jarvey.streams.model.TrackletId;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class AssociationCache extends LinkedHashMap<String,Association> {
	private static final long serialVersionUID = 1L;
	
	private final int m_maxSize;
	
	public AssociationCache(int maxSize) {
		super(maxSize, 0.75f, true);
		m_maxSize  = maxSize;
	}
	
	public FOption<Association> get(TrackletId trkId) {
		return get(trkId.toString());
	}
	
	public FOption<Association> get(String key) {
		Association assoc = super.get(key);
		if ( assoc == null ) {
			return FOption.empty();
		}
		else if ( assoc != EMPTY ) {
			return FOption.of(assoc);
		}
		else {
			return FOption.of(null);
		}
	}
	
	public void put(TrackletId trkId, Association assoc) {
		if ( assoc != null ) {
			assoc.getTracklets()
					.stream()
					.map(TrackletId::toString)
					.forEach(id -> super.put(id, assoc));
		}
		else {
			super.put(trkId.toString(), EMPTY);
		}
	}
	
	@Override
	protected boolean removeEldestEntry(Map.Entry<String, Association> eldest) {
		return size() > m_maxSize;
	}
	
	private static final Association EMPTY = new Association() {
		@Override
		public long getTimestamp() {
			return 0;
		}

		@Override
		public String getId() {
			return null;
		}

		@Override
		public Set<TrackletId> getTracklets() {
			return null;
		}

		@Override
		public double getScore() {
			return 0;
		}

		@Override
		public long getFirstTimestamp() {
			return 0;
		}

		@Override
		public Association removeTracklet(TrackletId trkId) {
			return null;
		}
		
	};
}
