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

    public void nextDocument() throws IOException {
        if (!done) {
            extentIterators[0].nextDocument();
            findDocument();
        }
    }

    public void findDocument() throws IOException {
        while (!done) {
            // find a document that might have some matches
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
            if (extents.getPosition() > 0) {
                break;
            }
            extentIterators[0].nextDocument();
        }
    }

    public ExtentArray extents() {
        return extents;
    }

    public int document() {
        return document;
    }

    public int count() {
        return extents().getPosition();
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
