/*
 * ScoreCombinationIteratorTest.java
 * JUnit based test
 *
 * Created on October 9, 2007, 2:43 PM
 */
package org.galagosearch.core.retrieval.extents;

import org.galagosearch.core.retrieval.structured.ScoreCombinationIterator;
import org.galagosearch.tupleflow.Parameters;
import java.io.IOException;
import junit.framework.TestCase;
import org.galagosearch.core.retrieval.structured.DocumentContext;
import org.galagosearch.core.retrieval.structured.ScoreValueIterator;

/**
 *
 * @author trevor
 */
public class ScoreCombinationIteratorTest extends TestCase {

  int[] docsA = new int[]{5, 10, 15, 20};
  double[] scoresA = new double[]{1.0, 2.0, 3.0, 4.0};
  int[] docsB = new int[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
  double[] scoresB = new double[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
  int[] docsTogether = new int[]{2, 4, 5, 6, 8, 10, 12, 14, 15, 16, 18, 20};
  double[] scoresTogether = new double[]{1, 2, 0.5, 3, 4, 6, 6, 7, 1.5, 8, 9, 12};
  // sjh: weight testing
  double[] weights = new double[]{0.2, 0.8};
  double[] weightedScoresTogether = new double[]{1.6, 3.2, 0.2, 4.8, 6.4, 8.4, 9.6, 11.2, 0.6, 12.8, 14.4, 16.8};

  public ScoreCombinationIteratorTest(String testName) {
    super(testName);
  }

  public ScoreCombinationIterator mockIterator(Parameters parameters, ScoreValueIterator[] iterators) {
    return new ScoreCombinationIterator(parameters, iterators) {

      public int currentCandidate() {
        throw new UnsupportedOperationException("Abstract method.");
      }

      public boolean hasMatch(int document) {
        return true;
      }

      public boolean isDone() {
        throw new UnsupportedOperationException("Abstract method.");
      }

      public boolean next() throws IOException {
        throw new UnsupportedOperationException("Abstract method.");
      }

      public long totalEntries() {
        throw new UnsupportedOperationException("Not supported yet.");
      }
    };
  }

  public void testScore() throws IOException {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
    FakeScoreIterator[] iterators = {one, two};

    Parameters anyParameters = new Parameters();
    ScoreCombinationIterator instance = mockIterator(anyParameters, iterators);

    for (int i = 0; i < 12; i++) {
      assertEquals(scoresTogether[i], instance.score(new DocumentContext(docsTogether[i], 100)));
      instance.movePast(docsTogether[i]);
    }
  }

  public void testWeightedScore() throws IOException {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
    FakeScoreIterator[] iterators = {one, two};

    Parameters weightParameters = new Parameters();
    weightParameters.add("0", Double.toString(weights[0]));
    weightParameters.add("1", Double.toString(weights[1]));
    ScoreCombinationIterator instance = mockIterator(weightParameters, iterators);

    for (int i = 0; i < 12; i++) {
      assert( Math.abs(weightedScoresTogether[i] - instance.score(new DocumentContext(docsTogether[i], 100))) < 0.000001);
      instance.movePast(docsTogether[i]);
    }
  }

  public void testMovePast() throws Exception {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
    FakeScoreIterator[] iterators = {one, two};

    Parameters anyParameters = new Parameters();
    ScoreCombinationIterator instance = mockIterator(anyParameters, iterators);
    instance.movePast(5);
  }

  public void testMoveTo() throws Exception {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
    FakeScoreIterator[] iterators = {one, two};

    Parameters anyParameters = new Parameters();
    ScoreCombinationIterator instance = mockIterator(anyParameters, iterators);

    instance.moveTo(5);
  }
}
