/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class FilteredCombinationIterator extends ScoreCombinationIterator {

  protected int document;
    
  public FilteredCombinationIterator(Parameters parameters, ScoreValueIterator[] childIterators) {
    super(parameters, childIterators);
    findDocument();
  }

    public boolean moveTo(int identifier) throws IOException {
	if (done) return false;
	for (ValueIterator iterator : iterators) {
	    iterator.moveTo(identifier);
	    if (iterator.isDone()) {
		document = Integer.MAX_VALUE;
		done = true;
		return false;
	    }
	}
	findDocument();
	return (done == false);
    }

    public void findDocument() throws IOException {
        if (!done) {
            // find a document that might have some matches
            document = MoveIterators.moveAllToSameDocument(iterators);

            // if we're done, quit now
            if (document == Integer.MAX_VALUE) {
                done = true;
                break;
            }
	}
    }

  public boolean next() throws IOException {
    if (!done) {
	iterators[0].next();
	findDocument();
    }
    return (done == false);
  }

    public void reset() throws IOException {
	super.reset();
	done = false;
	findDocument();
    }

  public long totalEntries() {
    long min = Long.MAX_VALUE;
    for (ValueIterator iterator : iterators) {
      min = Math.min(min, iterator.totalEntries());
    }
    return min;
  }
}
