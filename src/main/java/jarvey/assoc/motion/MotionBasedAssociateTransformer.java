package jarvey.assoc.motion;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import com.google.common.collect.Lists;

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
public class MotionBasedAssociateTransformer
	implements Transformer<String, NodeTrack,
							Iterable<KeyValue<String,Either<AssociationClosure,GlobalTrack>>>> {
	private NodeTrackWindower m_windower;
	private final GlobalTrackGenerator m_gtrackGenerator;
	private final List<KeyValue<String,Either<AssociationClosure,GlobalTrack>>> m_output = Lists.newArrayList();
	
	MotionBasedAssociateTransformer(OverlapAreaRegistry registry, Duration chopInterval,
										double assocDistanceThreshold, String storeName) {
		HoppingWindowManager windowMgr = HoppingWindowManager.ofWindowSize(chopInterval);
		m_windower = new NodeTrackWindower(windowMgr);

		AssociationClosureBuilder closureBuilder
				= new AssociationClosureBuilder(registry, assocDistanceThreshold);
		m_windower.addOutputConsumer(closureBuilder);
		closureBuilder.addOutputConsumer(this::addAssociationClosure);
		
		m_gtrackGenerator = new GlobalTrackGenerator(registry, closureBuilder.getClosureCollections());
		m_gtrackGenerator.addOutputConsumer(this::addGlobalTrack);
	}

	@Override
	public void init(ProcessorContext context) {
	}
	
	@Override
	public void close() {
		m_output.clear();
	}

	@Override
	public Iterable<KeyValue<String, Either<AssociationClosure,GlobalTrack>>>
	transform(String areaId, NodeTrack track) {
		m_output.clear();
		
		m_windower.accept(areaId, track);
		m_gtrackGenerator.accept(areaId, track);
		
		return Lists.newArrayList(m_output);
	}
	
	private void addAssociationClosure(String areaId, AssociationClosure cl) {
		m_output.add(KeyValue.pair(areaId, Either.left(cl)));
	}
	
	private void addGlobalTrack(String areaId, GlobalTrack gtrack) {
		m_output.add(KeyValue.pair(areaId, Either.right(gtrack)));
	}
}