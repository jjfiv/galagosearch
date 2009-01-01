// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import org.galagosearch.core.util.ExtentArray;

/**
 * This class is meant to be a base class for many kinds of
 * iterators that require at least one of their children to be
 * present in the document for a match to happen.  This class
 * will call loadExtents once for each document that has a
 * match on any child iterator.
 * 
 * @author trevor
 */
public abstract class ExtentDisjunctionIterator extends ExtentIterator {
    protected PriorityQueue<ExtentIterator> iterators;
    protected int document;
    protected ExtentArray extents;

    public ExtentDisjunctionIterator(ExtentIterator[] iterators) {
        this.iterators = new PriorityQueue<ExtentIterator>(iterators.length);

        for (ExtentIterator iterator : iterators) {
            if (!iterator.isDone()) {
                this.iterators.add(iterator);
            }
        }

        this.extents = new ExtentArray();
        this.document = 0;
    }

    public abstract void loadExtents();

    public void nextDocument() throws IOException {
        // find all iterators on the current document and move them forward
        while (iterators.size() > 0 && iterators.peek().document() == document) {
            ExtentIterator iter = iterators.poll();
            iter.nextDocument();

            if (!iter.isDone()) {
                iterators.offer(iter);
            }
        }

        if (!isDone()) {
            extents.reset();
            loadExtents();
        }
    }

    public boolean isDone() {
        return iterators.size() == 0;
    }

    public ExtentArray extents() {
        return extents;
    }

    public int document() {
        return document;
    }

    public int count() {
        return extents.getPosition();
    }

    public void reset() throws IOException {
        for (ExtentIterator iterator : iterators) {
            iterator.reset();
        }

        loadExtents();
    }
}
