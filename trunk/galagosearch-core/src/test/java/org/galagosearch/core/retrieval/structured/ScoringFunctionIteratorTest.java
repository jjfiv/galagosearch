/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.structured;

import junit.framework.TestCase;
import org.galagosearch.core.retrieval.extents.FakeExtentIterator;
import org.galagosearch.core.scoring.ScoringFunction;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author marc
 */
public class ScoringFunctionIteratorTest extends TestCase {

  int[][] extents = {
    {34, 55, 56, 57},
    {44, 67, 77},
    {110, 12, 23, 34}
  };

  private static class FakeScorer implements ScoringFunction {

    public double score(int count, int length) {
      return (count + length);
    }

    public String getParameterString() {
      return "fake";
    }
  }

  public void testGenericIterator() throws Exception {
    Parameters parameters = new Parameters();
    FakeExtentIterator extentIterator = new FakeExtentIterator(extents);
    ScoringFunctionIterator iterator = new ScoringFunctionIterator(extentIterator,
            new FakeScorer());
    assertFalse(iterator.isDone());
    assertEquals(extents[0][0], iterator.currentCandidate());
    iterator.moveTo(extents[0][0]);
    assertEquals(extents[0][0], iterator.currentCandidate());
    // score without context
    assertEquals(0.0, iterator.score());
    assertEquals(102.0, iterator.score(34, 99));
    iterator.movePast(44);
    assertTrue(iterator.hasMatch(110));
    assertEquals(99.0, iterator.score()); // length hasn't been reset
    assertEquals(44.0, iterator.score(110, 41));
    iterator.skipToDocument(120);
    assertTrue(iterator.isDone());
  }

  public void testBM25RFIterator() throws Exception {
    Parameters parameters = new Parameters();
    FakeExtentIterator extentIterator = new FakeExtentIterator(extents);
    Parameters p = new Parameters();
    p.set("rt", "3");
    p.set("R", "10");
    p.set("ft", "40");
    p.set("documentCount", "1000");
    p.set("factor", "0.45");
    BM25RFScoringIterator iterator = new BM25RFScoringIterator(p, extentIterator);
    assertFalse(iterator.isDone());
    assertEquals(extents[0][0], iterator.currentCandidate());
    iterator.moveTo(extents[0][0]);
    assertEquals(extents[0][0], iterator.currentCandidate());
    // score without context
    assertEquals(0.0, iterator.score());
    assertEquals(1.11315, iterator.score(34, 99), 0.0001);
    iterator.movePast(44);
    assertTrue(iterator.hasMatch(110));
    assertEquals(0.0, iterator.score()); // length hasn't been reset
    assertEquals(1.11315, iterator.score(110, 41), 0.0001);
    iterator.skipToDocument(120);
    assertTrue(iterator.isDone());
  }
}
