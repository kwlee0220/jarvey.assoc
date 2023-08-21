package jarvey.assoc.motion;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import utils.func.Either;
import utils.func.Funcs;
import utils.func.Lazy;
import utils.stream.FStream;

import jarvey.assoc.AssociationClosure;
import jarvey.assoc.AssociationClosure.Extension;
import jarvey.assoc.BinaryAssociation;
import jarvey.streams.model.TrackletDeleted;
import jarvey.streams.model.TrackletId;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AssociationClosureBuilder
	implements ValueTransformerWithKey<String, Either<BinaryAssociation,TrackletDeleted>,
										Iterable<AssociationClosure>> {
	private static final Logger s_logger = LoggerFactory.getLogger(AssociationClosureBuilder.class);
	private static final boolean KEEP_BEST_ASSOCIATION_ONLY = false;
	
	private final MotionBasedAssociatorContext m_mbaContext;
	private final Lazy<BinaryTrackAssociator> m_associator;
	private final Map<String,AssociationCollection<AssociationClosure>> m_cache = Maps.newHashMap();
	
	public AssociationClosureBuilder(MotionBasedAssociatorContext context) {
		m_mbaContext = context;
		m_associator = Lazy.of(this::getBinaryTrackAssociator);
	}
	
	private BinaryTrackAssociator getBinaryTrackAssociator() {
		return m_mbaContext.getBinaryTrackAssociator();
	}

	@Override
	public void init(ProcessorContext context) {
	}

	@Override
	public void close() {
	}
	
	public Map<String,AssociationCollection<AssociationClosure>> getClosureCollections() {
		return m_cache;
	}

	@Override
	public Iterable<AssociationClosure> transform(String areaId,
													Either<BinaryAssociation, TrackletDeleted> either) {
		if ( either.isLeft() ) {
			process(areaId, either.getLeft());
			return Collections.emptyList();
		}
		else {
			return process(areaId, either.getRight());
		}
	}
	
	private void process(String areaId, BinaryAssociation assoc) {
		List<AssociationClosure> extendedClosures = extend(areaId, assoc);
	}
	
	private List<AssociationClosure> process(String areaId, TrackletDeleted deleted) {
		List<AssociationClosure> fixedClosures = handleTrackDeleted(areaId, deleted);
		if ( fixedClosures.size() > 0 ) {
			Set<TrackletId> closedTracklets = FStream.from(fixedClosures)
													.flatMapIterable(cl -> cl.getTracklets())
													.toSet();
			
			// 최종적으로 선택된 association closure에 포함된 tracklet들과 연관된
			// 모든 binary association들을 제거한다.
			m_associator.get().purgeClosedBinaryAssociation(closedTracklets);
			
			// dangling tracklet들을 찾아 제거하고, 이에 해당하는 dangling association closure를
			// 생성/추가한다.
			FStream.from(m_associator.get().removeDanglingClosedTracklets())
					.map(ev -> AssociationClosure.singleton(ev.getTrackletId(), ev.getTimestamp()))
					.forEach(fixedClosures::add);
			
			return FStream.from(fixedClosures)
							.sort(AssociationClosure::getTimestamp)
							.toList();
		}
		else {
			return Collections.emptyList();
		}
	}
	
	private List<AssociationClosure> extend(String areaId, BinaryAssociation assoc) {
		AssociationCollection<AssociationClosure> collection
			= m_cache.computeIfAbsent(areaId, k -> new AssociationCollection<>(KEEP_BEST_ASSOCIATION_ONLY));
		
		List<AssociationClosure> matchingClosures = Funcs.removeIf(collection, assoc::intersectsTracklet);
		if ( matchingClosures.isEmpty() ) {
			AssociationClosure init = AssociationClosure.from(Arrays.asList(assoc));
			collection.add(init);
			return Arrays.asList(init);
		}
		else {
			List<AssociationClosure> extendeds = Lists.newArrayList();
			for ( AssociationClosure cl: matchingClosures ) {
				Extension ext = cl.extend(assoc, !KEEP_BEST_ASSOCIATION_ONLY);
				switch ( ext.type() ) {
					case UNCHANGED:
						collection.add(ext.association());
						break;
					case UPDATED:
					case EXTENDED:
						if ( collection.add(ext.association()) ) {
							extendeds.add(ext.association());
						}
						break;
					case CREATED:
						collection.add(cl);
						if ( collection.add(ext.association()) ) {
							extendeds.add(ext.association());
						}
						break;
				}
			}
			return extendeds;
		}
	}
	
	private Set<AssociationClosure> m_pendingFullClosers = Sets.newHashSet();
	private List<AssociationClosure> handleTrackDeleted(String areaId, TrackletDeleted deleted) {
		final AssociationCollection<AssociationClosure> collection
			= m_cache.computeIfAbsent(areaId, k -> new AssociationCollection<>(KEEP_BEST_ASSOCIATION_ONLY));
		
		final Set<TrackletId> closedTracklets = m_associator.get().getClosedTracklets();
		
		// 주어진 tracklet의 delete로 인해 해당 tracklet과 연관된 association들 중에서
		// fully closed된 association만 뽑는다.
		List<AssociationClosure> fullyCloseds
				= Funcs.filter(collection, cl -> closedTracklets.containsAll(cl.getTracklets()));
		// 뽑은 association들 중 일부는 서로 conflict한 것들이 있을 수 있기 때문에
		// 이들 중에서 점수를 기준으로 conflict association들을 제거한 best association 집합을 구한다.
		fullyCloseds = AssociationCollection.selectBestAssociations(fullyCloseds);
		if ( s_logger.isDebugEnabled() && fullyCloseds.size() > 0 ) { 
			for ( AssociationClosure cl: fullyCloseds ) {
				s_logger.debug("fully-closed: {}", cl);
			}
		}
		
		// 만일 유지 중인 association들 중에서 fully-closed 상태가 아니지만,
		// best association보다 superior한 것이 존재하면 해당 best association의 graduation을 대기시킴.
		List<AssociationClosure> graduated = Lists.newArrayList();
		while ( fullyCloseds.size() > 0 ) {
			AssociationClosure closed = Funcs.removeFirst(fullyCloseds);
			
			// close된 closure보다 더 superior한 closure가 있는지 확인하여
			// 없는 경우에만 관련 closure 삭제를 수행한다.
			AssociationClosure superior = collection.findSuperiorFirst(closed);
			if ( superior != null ) {
				if ( s_logger.isDebugEnabled() ) {
					s_logger.debug("found a superior: this={} superior={}", closed, superior);
				}
				
				// 'closed'보다 superior한 closure들이 최종 결과가 결정될 때까지 대기한다.
				// 더 superior한 closure가 close될 때는 그 closure가 처리될 때 자동적으로
				// 이 closure가 제거될 것이고, 만일 다른 fully-closed closure로 인해
				// 제거되는 경우를 별도 처리하기 위해 일단 따로 모은다.
				m_pendingFullClosers.add(closed);
			}
			else {
				graduate(collection, closed);
				graduated.add(closed);
			}
		}
		
		// pending되어 있던 closure 중에서 삭제를 막고 있던 closure가 또 다른 fully-closed
		// closure에 의해 삭제되었을 수도 있기 때문에, 삭제 여부를 확인한다.
		if ( graduated.size() > 0 && m_pendingFullClosers.size() > 0) {
			Funcs.removeIf(m_pendingFullClosers, fc -> {
				if ( collection.findSuperiorFirst(fc) == null ) {
					graduate(collection, fc);
					graduated.add(fc);
					return true;
				}
				else {
					return false;
				}
			});
		}
		
		return graduated;
	}
	
	private void graduate(AssociationCollection<AssociationClosure> collection, AssociationClosure closure) {
		// 졸업할 closure보다 inferior한 모든 closure들을 제거한다.
		List<AssociationClosure> removeds = collection.removeInferiors(closure);
		if ( removeds.size() > 0 ) {
			if ( s_logger.isDebugEnabled() ) {
				for ( AssociationClosure cl: removeds ) {
					s_logger.debug("removed an inferior for the graduated closure: removed={} superior={}", cl, closure);
				}
			}
				
			// 이 삭제로 pending list에 있는 closure도 삭제될 수 있기 때문에, pending list에서도 제거한다.
			m_pendingFullClosers.removeAll(removeds);
		}
		
		// graduate시키는 closure가 이전에 superior 때문에 graduate하지 못하고
		// m_pendingFullClosers에 포함되어 있을 수 있기 때문에 여기서도 제거한다.
		m_pendingFullClosers.remove(closure);
		
		collection.remove(closure.getTracklets());
	}
}
