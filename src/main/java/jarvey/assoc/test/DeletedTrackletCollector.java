package jarvey.assoc.test;

import java.util.Map;

import org.apache.kafka.streams.kstream.ForeachAction;

import jarvey.streams.model.TrackletDeleted;
import jarvey.streams.model.TrackletId;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DeletedTrackletCollector implements ForeachAction<String, TrackletDeleted> {
	private Map<TrackletId,TrackletDeleted> m_closedTracklets;
	
	public DeletedTrackletCollector(Map<TrackletId,TrackletDeleted> closedTracklets) {
		m_closedTracklets = closedTracklets;
	}
	
	public Map<TrackletId,TrackletDeleted> getClosedTracklets() {
		return m_closedTracklets;
	}

	@Override
	public void apply(String key, TrackletDeleted deleted) {
		m_closedTracklets.compute(deleted.getTrackletId(), (trkId, prev) -> {
			if ( prev == null ) {
				return deleted;
			}
			else if ( deleted.getTimestamp() < prev.getTimestamp() ) {
				return deleted;
			}
			else {
				return prev;
			}
		});
	}
}
