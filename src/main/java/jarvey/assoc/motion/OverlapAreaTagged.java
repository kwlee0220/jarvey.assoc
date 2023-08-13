package jarvey.assoc.motion;

import com.google.common.base.Objects;

import utils.Indexed;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class OverlapAreaTagged<T> {
	private final T m_value;
	private String m_overlapAreaId;
	
	public static <T> OverlapAreaTagged<T> tag(T value, String areaId) {
		return new OverlapAreaTagged<>(value, areaId);
	}
	
	private OverlapAreaTagged(T value, String areaId) {
		m_value = value;
		m_overlapAreaId = areaId;
	}
	
	public String areaId() {
		return m_overlapAreaId;
	}
	
	public T value() {
		return m_value;
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(m_overlapAreaId, m_value);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null  || obj.getClass() != Indexed.class ) {
			return false;
		}
		
		OverlapAreaTagged<T> other = (OverlapAreaTagged)obj;
		return m_overlapAreaId == other.m_overlapAreaId && m_value.equals(other.m_value);
	}
	
	@Override
	public String toString() {
		return String.format("[%s]: %s", m_overlapAreaId, m_value);
	}
}
