package jarvey.assoc.test;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;

import jarvey.assoc.AssociationCollection;
import jarvey.streams.model.AssociationClosure;
import jarvey.streams.model.AssociationClosure.Expansion;
import jarvey.streams.model.BinaryAssociation;
import jarvey.streams.model.TrackletId;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TestExtend {
	@SuppressWarnings("resource")
	public static void main(String... args) throws Exception {
		TrackletId T4_2 = new TrackletId("etri:04", "2");
		TrackletId T4_3 = new TrackletId("etri:04", "3");
		TrackletId T5_2 = new TrackletId("etri:05", "2");
		TrackletId T6_3 = new TrackletId("etri:06", "3");
		TrackletId T7_2 = new TrackletId("etri:07", "2");
		
		AssociationCollection collection = new AssociationCollection("test");
		
		BinaryAssociation ba1 = new BinaryAssociation("etri:05[2]", T5_2, T7_2, 0.99, 10, 10, 10);
		BinaryAssociation ba2 = new BinaryAssociation("etri:04[2]", T4_2, T6_3, 0.91, 10, 10, 10);
		BinaryAssociation ba3 = new BinaryAssociation("etri:04[3]", T4_3, T6_3, 0.23, 10, 10, 10);
		BinaryAssociation assoc = new BinaryAssociation("etri:05[2]", T5_2, T6_3, 0.86, 10, 10, 10);
		
		AssociationClosure cl1 = AssociationClosure.from(Arrays.asList(ba1));
		AssociationClosure cl2 = AssociationClosure.from(Arrays.asList(ba2));
		AssociationClosure cl3 = AssociationClosure.from(Arrays.asList(ba3));
		
		List<AssociationClosure> matchingClosures = Lists.newArrayList(cl1, cl2, cl3);
		for ( AssociationClosure cl: matchingClosures ) {
			Expansion ext = cl._expand(assoc, false);
			switch ( ext.type() ) {
				case UNCHANGED:
					break;
				case UPDATED:
				case CREATED:
					break;
				case EXTENDED:
					List<AssociationClosure> mergeds = Lists.newArrayList();
//					if ( collection.size() > 0 ) {
//						for ( AssociationClosure acl: collection ) {
//							AssociationClosure merged = acl.merge(ext.association());
//							if ( merged != null ) {
//								collection.add(merged);
//								mergeds.add(merged);
//							}
//							else {
//								mergeds.add(acl);
//							}
//						}
//					}
//					else {
						collection.add(ext.association());
//					}
					break;
			}
		}
	}
}
