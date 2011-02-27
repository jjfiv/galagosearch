// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.PriorityQueue;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.core.util.ExtentArray;

/**
 *
 * @author trevor
 */
public abstract class ExtentConjunctionIterator extends ExtentCombinationIterator {

    protected int document;
    protected boolean done;

  public ExtentConjunctionIterator(ExtentValueIterator[] extIterators) {
    this.done = false;
    iterators = new ExtentValueIterator[extIterators.length];
    for (int i = 0; i < extIterators.length; i++) {
	iterators[i] = extIterators[i];
    }
    this.extents = new ExtentArray();
  }

  // Move the lowest one forward first, then keep moving them forward
  // until they all match on the id.
  public boolean next() throws IOException {
      if (!done) {
	  iterators[0].next();
	  findDocument();
      }
      return (done == false);
  }

    public void findDocument() throws IOException {
        while (!done) {
            // find a document that might have some matches
            document = MoveIterators.moveAllToSameDocument(iterators);

            // if we're done, quit now
            if (document == Integer.MAX_VALUE) {
                done = true;
                break;
            }

            // try to load some extents (subclass does this)
            extents.reset();
            loadExtents();

            // were we successful? if so, quit, otherwise keep looking for documents
            if (extents.getPositionCount() > 0) {
                break;
            }
            iterators[0].next();
        }
    }

  public boolean isDone() {
    return done;
  }

  public boolean moveTo(int identifier) throws IOException {
    for (ValueIterator iterator : iterators) {
      iterator.moveTo(identifier);
    }
    findDocument();
  }

    public void reset() throws IOException {
        for (ExtentIterator iterator : iterators) {
            iterator.reset();
        }

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
