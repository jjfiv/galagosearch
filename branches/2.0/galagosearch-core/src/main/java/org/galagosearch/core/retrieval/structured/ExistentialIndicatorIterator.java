
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 * Implements the #any indicator operator.
 * @author irmarc
 */
public class ExistentialIndicatorIterator extends IndicatorIterator {

  public ExistentialIndicatorIterator(Parameters p, ValueIterator[] children) {
    super(p, children);
    updateState();
  }

  private void updateState() {
    int candidate = Integer.MAX_VALUE;
    for (ValueIterator iterator : iterators) {
      if (!iterator.isDone()) {
      candidate = Math.min(candidate, iterator.currentIdentifier());
      }
    }
    document = candidate;
    if (document == Integer.MAX_VALUE) {
      done = true;
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    updateState();
  }

  @Override
  public boolean getStatus() {
    return (context.document == this.document);
  }

  /**
   * Moves all iterators past the current internal document,
   * but it ONLY moves docs in that condition;
   * @return
   * @throws IOException
   */
  public boolean next() throws IOException {
    int newCandidate = Integer.MAX_VALUE;
    for (ValueIterator iterator : iterators) {
      while (iterator.currentIdentifier() <= document && !iterator.isDone()) {
        iterator.next();
      }
    }
    updateState();
    return (!done);
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
}
