package jarvey.assoc.motion;

import java.time.Duration;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import utils.func.Either;

import jarvey.assoc.AssociationClosure;
import jarvey.assoc.OverlapAreaRegistry;
import jarvey.streams.HoppingWindowManager;
import jarvey.streams.model.GlobalTrack;
import jarvey.streams.node.NodeTrack;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MotionBasedTrackAssociateProcessor implements Processor<String, NodeTrack,
															String, Either<AssociationClosure,GlobalTrack>> {
	private ProcessorContext<String, Either<AssociationClosure,GlobalTrack>> m_context;
	private NodeTrackWindower m_chopper;
	private final GlobalTrackGenerator m_gtrackGenerator;
	
	private final String m_assocPublisherChildName;
	private final String m_globalTrackPublisherChildName;
	
	MotionBasedTrackAssociateProcessor(OverlapAreaRegistry registry, Duration chopInterval,
									double assocDistanceThreshold, String storeName,
									String assocPublisherChildName,
									String globalTrackPublisherChildName) {
		HoppingWindowManager windowMgr = HoppingWindowManager.ofWindowSize(chopInterval);
		m_chopper = new NodeTrackWindower(windowMgr);

		AssociationClosureBuilder closureBuilder
				= new AssociationClosureBuilder(registry, assocDistanceThreshold);
		m_chopper.addOutputConsumer(closureBuilder);
		closureBuilder.addOutputConsumer(this::publishAssociationClosure);
		
		m_gtrackGenerator = new GlobalTrackGenerator(registry, closureBuilder.getClosureCollections());
		m_gtrackGenerator.addOutputConsumer(this::publishGlobalTrack);
		
		m_assocPublisherChildName = assocPublisherChildName;
		m_globalTrackPublisherChildName = globalTrackPublisherChildName;
	}

	@Override
	public void init(ProcessorContext<String, Either<AssociationClosure,GlobalTrack>> context) {
		m_context = context;
	}
	
	@Override
	public void close() {
	}

	@Override
	public void process(Record<String, NodeTrack> record) {
		String areaId = record.key();
		NodeTrack track = record.value();
		
		m_chopper.accept(areaId, track);
		m_gtrackGenerator.accept(areaId, track);
	}
	
	private void publishAssociationClosure(String areaId, AssociationClosure assoc) {
		Record<String, Either<AssociationClosure,GlobalTrack>> output
			= new Record<>(areaId, Either.left(assoc), assoc.getTimestamp());
		m_context.forward(output, m_assocPublisherChildName);
	}
	
	private void publishGlobalTrack(String areaId, GlobalTrack gtrack) {
		Record<String, Either<AssociationClosure,GlobalTrack>> output
			= new Record<>(areaId, Either.right(gtrack), gtrack.getTimestamp());
		m_context.forward(output, m_globalTrackPublisherChildName);
	}
}