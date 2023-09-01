package jarvey.assoc.feature;

import java.util.List;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class Utils {
	public static double norm(float[] vec) {
		double sum = 0;
		for ( float v: vec ) {
			sum += v*v;
		}
		
		return Math.sqrt(sum);
	}
	
	public static float[] normalize(float[] vec) {
		double norm = norm(vec);
		
		float[] normed = new float[vec.length];
		for ( int i =0; i < vec.length; ++i ) {
			normed[i] = (float)(vec[i] / norm);
		}
		
		return normed;
	}
	
	public static float dotProduct(float[] vec1, float[] vec2) {
		float sum = 0;
		for ( int i =0; i < vec1.length && i < vec2.length; ++i ) {
			sum += (vec1[i] * vec2[i]);
		}
		
		return sum;
	}
	
	public static double cosineSimilarityNormalized(float[] vec1, float[] vec2) {
		return dotProduct(vec1, vec2);
	}
	
	public static double cosineSimilarity(float[] vec1, float[] vec2) {
//		return dotProduct(vec1, vec2) / (norm(vec1) * norm(vec2));
		vec1 = normalize(vec1);
		vec2 = normalize(vec2);
		return cosineSimilarityNormalized(vec1, vec2);
	}
	
	public static double cosineSimilarity(float[] vec1, float[][] mat2, int kth) {
		MinMaxPriorityQueue<Double> heap = MinMaxPriorityQueue.maximumSize(kth)
																.create();
		for ( int ri = 0; ri < mat2.length; ++ri ) {
			heap.add(cosineSimilarity(vec1, mat2[ri]));
		}
		
		double kthValue = 0;
		for ( int i =0; i < kth; ++i ) {
			kthValue = heap.pollFirst();
		}
		return kthValue;
	}
	
	public static class Match implements Comparable<Match> {
		private final int m_leftIndex;
		private final int m_rightIndex;
		private final double m_score;
		
		Match(int leftIndex, int rightIndex, double score) {
			m_leftIndex = leftIndex;
			m_rightIndex = rightIndex;
			m_score = score;
		}
		
		public int getLeftIndex() {
			return m_leftIndex;
		}
		
		public int getRightIndex() {
			return m_rightIndex;
		}
		
		public double getScore() {
			return m_score;
		}

		@Override
		public int compareTo(Match o) {
			return Double.compare(m_score,  o.m_score);
		}

		@Override
		public String toString() {
			return String.format("[%d<->%d] %.3f", m_leftIndex, m_rightIndex, m_score);
		}
	}
	
	public static Match cosineSimilarityNormalized(List<float[]> mat1, List<float[]> mat2,
													double percent) {
		int kth = (int)Math.floor((mat1.size() * mat2.size()) * percent);
		kth = Math.max(kth, 1);
		MinMaxPriorityQueue<Match> heap = MinMaxPriorityQueue.orderedBy(Ordering.natural())
																.maximumSize(kth)
																.create();
		for ( int i =0; i < mat1.size(); ++i ) {
			for ( int j =0; j < mat2.size(); ++j ) {
				double score = cosineSimilarity(mat1.get(i), mat2.get(j));
				heap.add(new Match(i, j, score));
			}
		}
		
		return heap.pollLast();
	}
	
	public static Match calcTopKDistance(List<float[]> mat1, List<float[]> mat2, double percent) {
		Match match = cosineSimilarityNormalized(mat1, mat2, percent);
		return new Match(match.m_leftIndex, match.m_rightIndex, 1-match.m_score);
	}
}
