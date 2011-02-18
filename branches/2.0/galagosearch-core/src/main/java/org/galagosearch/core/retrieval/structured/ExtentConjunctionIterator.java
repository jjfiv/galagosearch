// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.ArrayList;
import org.galagosearch.core.util.ExtentArray;

/**
 *
 * @author trevor
 */
public abstract class ExtentConjunctionIterator extends ExtentIterator {
    protected ExtentIterator[] extentIterators;
    protected ExtentArray extents;
    protected int document;
    protected boolean done;

    public ExtentConjunctionIterator(ExtentIterator[] extIterators) {
        this.done = false;
        this.extentIterators = extIterators;
        this.extents = new ExtentArray();
    }

    public abstract void loadExtents();

    public void nextEntry() throws IOException {
        if (!done) {
            extentIterators[0].nextEntry();
            findDocument();
        }
    }

    public void findDocument() throws IOException {
        while (!done) {
            // find a identifier that might have some matches
            document = MoveIterators.moveAllToSameDocument(extentIterators);

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
            extentIterators[0].nextEntry();
        }
    }

    public ExtentArray extents() {
        return extents;
    }

    public int identifier() {
        return document;
    }

    public int count() {
        return extents().getPositionCount();
    }

    public boolean isDone() {
        return done;
    }

    public void reset() throws IOException {
        for (ExtentIterator iterator : extentIterators) {
            iterator.reset();
        }

        done = false;
        findDocument();
    }
}
