package jarvey.assoc.motion;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import com.google.common.collect.Maps;

import utils.func.Either;

import jarvey.assoc.BinaryAssociationStore;
import jarvey.streams.model.AssociationClosure;
import jarvey.streams.model.BinaryAssociation;
import jarvey.streams.model.TrackletDeleted;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class Processors {
	static class ContextSetter
					implements ValueTransformer<Either<BinaryAssociation,TrackletDeleted>,
												Iterable<Either<BinaryAssociation,TrackletDeleted>>> {
		private final MotionBasedAssociationContext m_context;
		private final String m_storeName;
		private final boolean m_useMockStore;
		
		public ContextSetter(MotionBasedAssociationContext context, String storeName, boolean useMockStore) {
			m_context = context;

			m_storeName = storeName;
			if ( useMockStore ) {
				m_context.setBinaryAssociationStore(BinaryAssociationStore.createLocalStore(storeName));
			}
			
			m_useMockStore = useMockStore;
		}

		@Override
		public void init(ProcessorContext context) {
			if ( !m_useMockStore ) {
				BinaryAssociationStore store = BinaryAssociationStore.fromStateStore(context, m_storeName);
				m_context.setBinaryAssociationStore(store);
			}
		}

		@Override
		public Iterable<Either<BinaryAssociation, TrackletDeleted>>
		transform(Either<BinaryAssociation, TrackletDeleted> either) {
			return Collections.singleton(either);
		}

		@Override
		public void close() { }
	}
	
	static class FixedAssociationSelectorManager
					implements ValueMapperWithKey<String, TrackletDeleted, Iterable<AssociationClosure>> {
		private final MotionBasedAssociationContext m_context;
		private final Map<String,FinalAssociationSelector> m_selectors = Maps.newHashMap();
		
		public FixedAssociationSelectorManager(MotionBasedAssociationContext context) {
			m_context = context;
		}
	
		@Override
		public Iterable<AssociationClosure> apply(String areaId, TrackletDeleted deleted) {
			FinalAssociationSelector associator = m_selectors.get(areaId);
			if ( associator == null ) {
				associator = new FinalAssociationSelector(m_context.getOrCreateSession(areaId));
				m_selectors.put(areaId, associator);
			}
			
			return associator.select(deleted);
		}
	}

	static class AssociationExtenderManager
					implements ValueMapperWithKey<String, BinaryAssociation, Iterable<AssociationClosure>> {
		private final MotionBasedAssociationContext m_context;
		private final Map<String,AssociationExpander> m_extenders = Maps.newHashMap();
		
		public AssociationExtenderManager(MotionBasedAssociationContext context) {
			m_context = context;
		}
	
		@Override
		public Iterable<AssociationClosure> apply(String areaId, BinaryAssociation assoc) {
			AssociationExpander extender = m_extenders.get(areaId);
			if ( extender == null ) {
				extender = new AssociationExpander(m_context.getOrCreateSession(areaId));
				m_extenders.put(areaId, extender);
			}
			
			return extender.extend(assoc);
		}
	}
}
