package jarvey.assoc.test;

import java.util.Collections;
import java.util.Set;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import utils.func.Either;

import jarvey.streams.model.JarveySerdes;
import jarvey.streams.model.TrackFeatureSerde;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.node.TrackFeature;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class Processors {
	static class ContextSetter implements ValueTransformerWithKey<String, byte[],
														Iterable<Either<NodeTrack,TrackFeature>>> {
		private ProcessorContext m_kafkaContext;
		private final Set<String> m_listeningNodes;
		
		private final Deserializer<NodeTrack> NODE_TRACK_DESER = JarveySerdes.NodeTrack().deserializer();
		
		public ContextSetter(Set<String> listeningNodes) {
			m_listeningNodes = listeningNodes;
		}

		@Override
		public void init(ProcessorContext context) {
			m_kafkaContext = context;
		}

		@Override
		public Iterable<Either<NodeTrack,TrackFeature>> transform(String nodeId, byte[] bytes) {
			String topic = m_kafkaContext.topic();
			Either<NodeTrack,TrackFeature> either = null;
			switch ( m_kafkaContext.topic() ) {
				case "track-features":
					if ( !m_listeningNodes.contains(nodeId) ) {
						return Collections.emptyList();
					}
					
					TrackFeature tfeat = TrackFeatureSerde.s_deerializer.deserialize(topic, bytes);
					either = Either.right(tfeat);
					break;
				case "node-tracks":
					NodeTrack track = NODE_TRACK_DESER.deserialize(topic, bytes);
					either = Either.left(track);
					break;
				default:
					throw new IllegalArgumentException(String.format("unknown topic: %s",
																		m_kafkaContext.topic()));
			}
			return Collections.singletonList(either	);
		}

		@Override
		public void close() { }
	}
}
