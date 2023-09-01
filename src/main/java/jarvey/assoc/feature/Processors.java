package jarvey.assoc.feature;

import java.util.Collections;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import utils.func.Either;

import jarvey.assoc.BinaryAssociationStore;
import jarvey.streams.model.JarveySerdes;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.node.TrackFeature;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
class Processors {
	static class ContextSetter implements ValueTransformer<TrackFeature, Iterable<TrackFeature>> {
		private final FeatureBasedAssociationContext m_context;
		private final String m_storeName;
		private final boolean m_useMockStore;
		
		ContextSetter(FeatureBasedAssociationContext context, String storeName, boolean useMockStore) {
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
		public void close() { }
	
		@Override
		public Iterable<TrackFeature> transform(TrackFeature tfeat) {
			return Collections.singleton(tfeat);
		}
	}
	
	static class Demux implements ValueTransformer<byte[], Iterable<Either<TrackFeature,NodeTrack>>> {
		private ProcessorContext m_context;
		private final Deserializer<TrackFeature> m_featureDeserializer = TrackFeatureSerde.s_deerializer;
		private final Deserializer<NodeTrack> m_trackDeserializer = JarveySerdes.NodeTrack().deserializer();

		@Override
		public void init(ProcessorContext context) {
			m_context = context;
		}

		@Override
		public void close() { }

		@Override
		public Iterable<Either<TrackFeature,NodeTrack>> transform(byte[] bytes) {
			Either<TrackFeature,NodeTrack> either;
			
			String topic = m_context.topic();
			if ( topic.equals("track-features") ) {
				either = Either.left(m_featureDeserializer.deserialize(topic, bytes));
			}
			else {
				either = Either.right(m_trackDeserializer.deserialize(topic, bytes));
			}
			
			return Collections.singleton(either);
		}
	}
}
