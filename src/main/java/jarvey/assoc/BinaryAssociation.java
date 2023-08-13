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
	@SerializedName("left_track") private TrackletId m_leftTrackId;
	@SerializedName("right_track") private TrackletId m_rightTrackId;
	@SerializedName("score") private double m_score;
	@SerializedName("left_ts") private long m_leftTs;
	@SerializedName("right_ts") private long m_rightTs;
	
	public BinaryAssociation(TrackletId left, TrackletId right, double score, long leftTs, long rightTs) {
		if ( left.compareTo(right) < 0 ) {
			m_leftTrackId = left;
			m_rightTrackId = right;
			m_leftTs = leftTs;
			m_rightTs = rightTs;
		}
		else {
			m_leftTrackId = right;
			m_rightTrackId = left;
			m_leftTs = rightTs;
			m_rightTs = leftTs;
		}
		m_score = score;
	}

	@Override
	public Set<TrackletId> getTracklets() {
		return Sets.newHashSet(m_leftTrackId, m_rightTrackId);
	}
	
	public TrackletId getLeftTrackId() {
		return m_leftTrackId;
	}
	
	public TrackletId getRightTrackId() {
		return m_rightTrackId;
	}
	
	public int indexOf(TrackletId trkId) {
		if ( m_leftTrackId.equals(trkId) ) {
			return 0;
		}
		else if ( m_rightTrackId.equals(trkId) ) {
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
		return m_leftTs;
	}
	
	public long getRightTimestamp() {
		return m_rightTs;
	}
	
	@Override
	public long getTimestamp() {
		return Math.max(m_leftTs, m_rightTs);
	}

	public boolean match(BinaryAssociation other) {
		return Objects.equals(m_leftTrackId, other.m_leftTrackId)
				&& Objects.equals(m_rightTrackId, other.m_rightTrackId);
	}
	
	@Override
	public BinaryAssociation removeTracklet(TrackletId trkId) {
		if ( m_leftTrackId.equals(trkId) || m_rightTrackId.equals(trkId) ) {
			return null;
		}
		else {
			return this;
		}
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_leftTrackId, m_rightTrackId, m_score, m_leftTs, m_rightTs);
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
		
		return m_leftTrackId.equals(other.m_leftTrackId)
				&& m_rightTrackId.equals(other.m_rightTrackId)
				&& Double.compare(m_score, other.m_score) == 0
				&& m_leftTs == other.m_leftTs
				&& m_rightTs == other.m_rightTs;
	}
	
	@Override
	public String toString() {
		return String.format("%s-%s:%.2f#%d",
								m_leftTrackId, m_rightTrackId, m_score, getTimestamp());
	}
}
