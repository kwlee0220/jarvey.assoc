package jarvey.assoc.feature;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import utils.func.Either;
import utils.func.Funcs;
import utils.func.Tuple;
import utils.func.Tuple3;
import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;
import utils.stream.KeyedGroups;

import jarvey.assoc.feature.FeatureBinaryTrackletAssociator.MatchingSession.State;
import jarvey.assoc.feature.MCMOTNetwork.IncomingLink;
import jarvey.assoc.feature.Utils.Match;
import jarvey.streams.model.BinaryAssociation;
import jarvey.streams.model.BinaryAssociationCollection;
import jarvey.streams.model.TrackFeatureSerde;
import jarvey.streams.model.TrackletDeleted;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrackletIndex;
import jarvey.streams.node.NodeTrackletUpdateLogs;
import jarvey.streams.node.TrackFeature;
import jarvey.streams.updatelog.KeyedUpdateIndex;
import jarvey.streams.updatelog.KeyedUpdateLogs;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class FeatureBinaryTrackletAssociator
		implements ValueMapper<TrackFeature, Iterable<Either<BinaryAssociation,TrackletDeleted>>> {
	private static final Logger s_logger = LoggerFactory.getLogger(FeatureBinaryTrackletAssociator.class);

	private final MCMOTNetwork m_network = new MCMOTNetwork();
	private final KeyedUpdateLogs<TrackFeature> m_indexStore;
	private final NodeTrackletUpdateLogs m_trackletIndexes;
	private final double m_topPercent;
	
	private BinaryAssociationCollection m_binaryCollection;
	private Map<TrackletId, Candidate> m_candidates = Maps.newHashMap();
	private Map<TrackletId, MatchingSession> m_sessions = Maps.newHashMap();

	public FeatureBinaryTrackletAssociator(JdbcProcessor jdbc, Properties consumerProps,
											double topPercent, BinaryAssociationCollection binaryCollection) {
		Deserializer<TrackFeature> featureDeser = TrackFeatureSerde.s_deerializer;
		m_indexStore = new KeyedUpdateLogs<>(jdbc, "track_features_index", consumerProps,
											"track-features", featureDeser);
		m_trackletIndexes = new NodeTrackletUpdateLogs(jdbc, "node_tracks_index",
														consumerProps, "node-tracks");
		m_topPercent = topPercent;
		m_binaryCollection = binaryCollection;
	}
	
	@Override
	public Iterable<Either<BinaryAssociation,TrackletDeleted>> apply(TrackFeature tfeat) {
		if ( tfeat.isDeleted() ) {
			tearDownSession(tfeat.getTrackletId());
			
			TrackletDeleted deleted = TrackletDeleted.of(tfeat.getTrackletId(), tfeat.getTimestamp());
			return Collections.singleton(Either.right(deleted));
		}
		
		MatchingSession session = createOrUpdateSession(tfeat);
		if ( session.getState() != State.ACTIVATED ) {
			return Collections.emptyList();
		}
		
		List<BinaryAssociation> assocList = Lists.newArrayList();
		for ( TrackletId candTrkId: session.m_candidates ) {
			Candidate candidate = m_candidates.get(candTrkId);
			if ( candidate != null && candidate.m_trackFeatures.size() > 0 ) {
				Tuple3<Long,Long,Double> ret = calcTopKDistance(session, candidate);
				String id = candidate.getTrackletId().toString();
				
				BinaryAssociation assoc;
				if ( tfeat.getTrackletId().compareTo(candidate.getTrackletId()) <= 0 ) {
					assoc = new BinaryAssociation(id, tfeat.getTrackletId(), candidate.getTrackletId(),
							 						ret._3, ret._1, ret._2, candidate.getStartTimestamp());
				}
				else {
					assoc = new BinaryAssociation(id, candidate.getTrackletId(), tfeat.getTrackletId(),
			 										ret._3, ret._2, ret._1, candidate.getStartTimestamp());
				}
				if ( m_binaryCollection.add(assoc) ) {
					assocList.add(assoc);
				}
			}
		}
		
		return Funcs.map(assocList, Either::left);
	}
	
	private MatchingSession createOrUpdateSession(TrackFeature tfeature) {
		TrackletId trkId = tfeature.getTrackletId();
		
		MatchingSession session = m_sessions.computeIfAbsent(trkId, MatchingSession::new);
		if ( session.getState() == State.DISABLED ) {
			return session;
		}
		
		// possible states: STATE_NOT_READY, STATE_ACTIVATED
		session.addTrackFeature(tfeature);
		
		if ( session.m_enterZone == null ) {
			NodeTrackletIndex trackletIndex = m_trackletIndexes.getIndex(trkId);
			if ( trackletIndex != null ) {
				session.m_startTs = trackletIndex.getTimestampRange().min();
				session.m_enterZone = trackletIndex.getEnterZone();
				if ( session.m_enterZone != null && s_logger.isDebugEnabled() ) {
					s_logger.debug("The watching tracklet' enter-zone is ready: id={}, zone={}",
									session.m_trkId, session.m_enterZone);
				}
			}
		}
		if ( session.m_enterZone == null ) {
			if ( s_logger.isDebugEnabled() ) {
				s_logger.debug("The watching tracklet' enter-zone is not ready: id={}", trkId);
			}
			
			session.m_state = State.NOT_READAY;
			return session;
		}
		
		// 모든 incoming link 정보에 해당하는 node들에서 적절한 기간동안 지정된
		// zone을 통해 exit한 tracklet에 대한 track-feature 들을 모두 수집한다.
		if ( session.m_enterZone != null && session.getState() == State.NOT_READAY ) {
			// 본 tracklet의 enter-zone 정보를 기반으로 동일 tracklet이 바로 전에
			// 등장했을 만한 노드(카메라) 정보를 얻는다. 
			List<IncomingLink> incomingLinks = m_network.getIncomingLinks(session.m_trkId.getNodeId(),
																			session.m_enterZone);
			if ( incomingLinks == null ) {
				// 예측되는 이전 카메라 정보가 없는 경우는 추적을 포기한다.
				session.setState(State.DISABLED);
			}
			else {
				if ( s_logger.isDebugEnabled() ) {
					s_logger.debug("found the previous cameras: {}:{}->, {}",
									trkId, session.m_enterZone, incomingLinks);
				}
				
				for ( IncomingLink link: incomingLinks ) {
					// 이전 카메라서에서 추적됐을 것으로 예측되는 tracklet의 feature 정보들을
					// download하고 matching 후보로 설정한다.
					List<TrackletId> candidateTrkIds = prepareCandidateFeatures(session.m_startTs, link);
					session.m_candidates.addAll(candidateTrkIds);
				}
				
				if ( s_logger.isDebugEnabled() ) {
					List<Tuple<TrackletId,Integer>> tfeats
						= FStream.from(session.m_candidates)
									.map(tid -> Tuple.of(tid, m_candidates.get(tid).m_featureList.size()))
									.toList();
					s_logger.debug("calculate distances: {}:{}->, {}", trkId, session.m_enterZone, tfeats);
				}
				
				// association에 필요한 모든 후보 tracklet들에 대한 feature들에 수집하였기 때문에
				// association을 시작할 수 있을 알린다.
				session.setState(State.ACTIVATED);
			}
		}
		
		return session;
	}
	
	private List<TrackletId> prepareCandidateFeatures(long enterTs, IncomingLink link) {
		// 이전 노드에서의 candidate tracklet의 예상 exit 시간 구간을 계산하여
		// 해당 구간에 exit한 tracklet들을 후보들의 NodeTrackletIndex 정보는 읽어온다.
		long ts = enterTs - link.getTransitionTime().toMillis();
		String whereClause
			= String.format("node = '%s' and exit_zone='%s' and last_ts between %d and %d",
							link.getExitNode(), link.getExitZone(), ts - 4*1000, ts+1000);
		
		List<TrackletId> candidateTrkIds
			= Funcs.map(m_trackletIndexes.findIndexes(whereClause), NodeTrackletIndex::getTrackletId);
		if ( candidateTrkIds.size() > 0 ) {
			//
			// tracklet index를 바탕으로 해당 tracklet의 feature를 뽑는다.
			// 이때, 효과적인 TrackFeature 접근을 위해, 이미 캐슁된 tracklet과
			// 캐슁되지 않는 tracklet을 따로 분리하여 처리한다.
			//
			KeyedGroups<Boolean, TrackletId> cases = FStream.from(candidateTrkIds)
															.groupByKey(k -> m_candidates.get(k) != null);
			cases.switcher()
				 .ifCase(true).consume(this::shareCandidate)	// 이미 cache된 경우는 share-count만 증가.
				 .ifCase(false).consume(this::downloadCandidateFeatures);
		}
		
		return candidateTrkIds;
	}
	
	private void downloadCandidateFeatures(List<TrackletId> trkIds) {
		List<TrackletId> missingTrkIds = Funcs.asNonNull(trkIds, Collections.emptyList());
		if ( missingTrkIds.size() > 0 ) {
			List<String> missingKeys = Funcs.map(missingTrkIds, TrackletId::toString);
			for ( KeyedUpdateIndex index: m_indexStore.readIndexes(missingKeys).values() ) {
				if ( !index.isClosed() ) {
					if ( s_logger.isDebugEnabled() ) {
						s_logger.debug("skip for the unfinished tracklet's features: id={}", index.getKey());
					}
					continue;
				}

				TrackletId trkId = TrackletId.fromString(index.getKey());
				Candidate candidate = new Candidate(trkId);
				m_indexStore.streamOfIndex(index)
				 			.map(kv -> kv.value)
				 			.filterNot(TrackFeature::isDeleted)
				 			.forEach(candidate::addTrackFeature);
				 m_candidates.put(trkId, candidate);
				if ( s_logger.isInfoEnabled() ) {
					s_logger.info("candidate tracklet's features are ready: target={}, nfeatures={}",
									index.getKey(), candidate.getFeatureCount());
				}
			}
		}
	}
	
	private void shareCandidate(List<TrackletId> trkIds) {
		trkIds.forEach(trkId -> {
			Candidate candidate = m_candidates.get(trkId);
			candidate.incrementShareCount();
		});
	}
	
	private void tearDownSession(TrackletId trkId) {
		MatchingSession session = m_sessions.remove(trkId);
		if ( session == null ) {
			return;
		}
		
		for ( TrackletId candTrkId: session.m_candidates ) {
			Candidate candidate = m_candidates.get(candTrkId);
			if ( candidate != null ) {
				if ( --candidate.m_shareCount == 0 ) {
					m_candidates.remove(candTrkId);
				}
			}
		}
	}
	
	private Tuple3<Long,Long,Double> calcTopKDistance(MatchingSession session, Candidate candidate) {
		Match match = Utils.calcTopKDistance(session.m_featureList, candidate.m_featureList, m_topPercent);
		long leftTs = session.m_trackFeatures.get(match.getLeftIndex()).getTimestamp();
		long rightTs = candidate.m_trackFeatures.get(match.getRightIndex()).getTimestamp();
		
		return Tuple.of(leftTs, rightTs, match.getScore());
	}

	private static class Candidate {
		private final TrackletId m_trkId;
		private int m_shareCount;
		private final List<TrackFeature> m_trackFeatures;
		private final List<float[]> m_featureList;

		Candidate(TrackletId trkId) {
			m_trkId = trkId;
			m_shareCount = 1;
			m_trackFeatures = Lists.newArrayList();
			m_featureList = Lists.newArrayList();
		}
		
		TrackletId getTrackletId() {
			return m_trkId;
		}
		
		long getStartTimestamp() {
			return m_trackFeatures.get(0).getTimestamp();
		}
		
		int getFeatureCount() {
			return m_featureList.size();
		}
		
		void addTrackFeature(TrackFeature tfeat) {
			m_trackFeatures.add(tfeat);
			m_featureList.add(tfeat.getFeature());
		}
		
		void incrementShareCount() {
			++m_shareCount;
		}
		
		@Override
		public String toString() {
			return String.format("%s: nfeats=%d", m_trkId, m_trackFeatures.size());
		}
	}

	static class MatchingSession {
		private final TrackletId m_trkId;
		private String m_enterZone;
		private long m_startTs;
		private final List<TrackFeature> m_trackFeatures;
		private final List<float[]> m_featureList;
		private State m_state = State.NOT_READAY;
		private Set<TrackletId> m_candidates = Sets.newHashSet();
		
		static enum State {
			NOT_READAY,
			ACTIVATED,
			DISABLED,
		};

		MatchingSession(TrackletId trkId) {
			m_trkId = trkId;
			m_trackFeatures = Lists.newArrayList();
			m_featureList = Lists.newArrayList();
		}
		
		State getState() {
			return m_state;
		}
		
		void setState(State state) {
			m_state = state;
		}
		
		void addTrackFeature(TrackFeature tfeat) {
			m_trackFeatures.add(tfeat);
			m_featureList.add(tfeat.getFeature());
		}
		
		@Override
		public String toString() {
			return String.format("%s: enter=%s, nfeats=%d, candidate=%s",
								m_trkId, m_enterZone, m_trackFeatures.size(), m_candidates);
		}
	}
}
