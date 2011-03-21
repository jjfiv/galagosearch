
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 * Implements the #any indicator operator.
 * @author irmarc
 */
public class ExistentialIndicatorIterator extends IndicatorIterator {

  private int document;
  private boolean done;

  public ExistentialIndicatorIterator(Parameters p, ValueIterator[] children) {
    super(p, children);
    updateState();
  }

  private void updateState() {
    int candidate = Integer.MAX_VALUE;
    for (ValueIterator iterator : iterators) {
      if (!iterator.isDone()) {
      candidate = Math.min(candidate, iterator.currentCandidate());
      }
    }
    document = candidate;
    if (document == Integer.MAX_VALUE) {
      done = true;
    }
  }

  public int currentCandidate() {
    return document;
  }

  public boolean isDone(){
    return done;
  }

  @Override
  public void reset() throws IOException {
    for(ValueIterator i : iterators){
      i.reset();
    }
    done = false;
    updateState();
  }

  @Override
  public boolean getStatus() {
    return (context.document == this.document);
  }

  public boolean moveTo(int identifier) throws IOException {
    boolean success = false;
    for (ValueIterator iterator : iterators) {
      success |= iterator.moveTo(identifier);
    }
    updateState();
    return success;
  }

  public String getEntry() throws IOException {
    return Integer.toString(document);
  }

  /**
   * Uses the max of all lists. This is inaccurate, but taking the union is
   * a horrible idea.
   * @return
   */
  public long totalEntries() {
    long max = Integer.MIN_VALUE;
    for (ValueIterator iterator : iterators) {
      max = Math.max(max, iterator.totalEntries());
    }
    return max;
  }

  /**
   * Moves all iterators past the current internal document,
   * but it ONLY moves docs in that condition;
   *
   *  *** BE VERY CAREFUL IN CALLING THIS FUNCTION ***
   *
   * @return
   * @throws IOException
   */
  public boolean next() throws IOException {
    for (ValueIterator iterator : iterators) {
      movePast(document);
    }
    updateState();
    return (!done);
  }
}
