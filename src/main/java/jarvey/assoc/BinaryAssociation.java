package jarvey.assoc;

import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;

import jarvey.streams.model.TrackletId;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class BinaryAssociation implements Association {
	@SerializedName("left") private TimedTracklet m_left;
	@SerializedName("right") private TimedTracklet m_right;
	@SerializedName("score") private double m_score;
//	@SerializedName("left_track") private TrackletId m_leftTrackId;
//	@SerializedName("right_track") private TrackletId m_rightTrackId;
//	@SerializedName("score") private double m_score;
//	@SerializedName("left_ts") private long m_leftTs;
//	@SerializedName("right_ts") private long m_rightTs;
	
	private static class TimedTracklet {
		@SerializedName("tracklet") private TrackletId m_tracklet;
		@SerializedName("ts") private long m_ts;
		
		TimedTracklet(TrackletId trkId, long ts) {
			m_tracklet = trkId;
			m_ts = ts;
		}
	}
	
	public BinaryAssociation(TrackletId left, TrackletId right, double score, long leftTs, long rightTs) {
		if ( left.compareTo(right) < 0 ) {
			m_left = new TimedTracklet(left, leftTs);
			m_right = new TimedTracklet(right, rightTs);
		}
		else {
			m_right = new TimedTracklet(left, leftTs);
			m_left = new TimedTracklet(right, rightTs);
		}
		m_score = score;
	}

	@Override
	public Set<TrackletId> getTracklets() {
		return Sets.newHashSet(m_left.m_tracklet, m_right.m_tracklet);
	}
	
	public TrackletId getLeftTrackId() {
		return m_left.m_tracklet;
	}
	
	public TrackletId getRightTrackId() {
		return m_right.m_tracklet;
	}
	
	public int indexOf(TrackletId trkId) {
		if ( m_left.m_tracklet.equals(trkId) ) {
			return 0;
		}
		else if ( m_right.m_tracklet.equals(trkId) ) {
			return 1;
		}
		else {
			throw new IllegalArgumentException(String.format("invalid tracklet: %s", trkId));
		}
	}
	
	public TrackletId getOther(TrackletId trkId) {
		return trkId.equals(getLeftTrackId()) ? getRightTrackId() : getLeftTrackId();
	}

	@Override
	public double getScore() {
		return m_score;
	}
	
	public long getLeftTimestamp() {
		return m_left.m_ts;
	}
	
	public long getRightTimestamp() {
		return m_right.m_ts;
	}
	
	@Override
	public long getTimestamp() {
		return Math.max(getLeftTimestamp(), getRightTimestamp());
	}

	public boolean match(BinaryAssociation other) {
		return Objects.equals(getLeftTrackId(), other.getLeftTrackId())
				&& Objects.equals(getRightTrackId(), other.getRightTrackId());
	}
	
	@Override
	public BinaryAssociation removeTracklet(TrackletId trkId) {
		if ( getLeftTrackId().equals(trkId) || getRightTrackId().equals(trkId) ) {
			return null;
		}
		else {
			return this;
		}
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(getLeftTrackId(), getRightTrackId(), m_score, getLeftTimestamp(), getRightTimestamp());
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		BinaryAssociation other = (BinaryAssociation)obj;
		
		return getLeftTrackId().equals(other.getLeftTrackId())
				&& getRightTrackId().equals(other.getRightTrackId())
				&& Double.compare(m_score, other.m_score) == 0
				&& getLeftTimestamp() == other.getLeftTimestamp()
				&& getRightTimestamp() == other.getRightTimestamp();
	}
	
	@Override
	public String toString() {
		return String.format("%s-%s:%.2f#%d",
							getLeftTrackId(), getRightTrackId(), m_score, getTimestamp());
	}
}
