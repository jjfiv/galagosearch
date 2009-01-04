// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class ExtentInsideIterator extends ExtentConjunctionIterator {
    ExtentIterator innerIterator;
    ExtentIterator outerIterator;

    /** Creates a new instance of ExtentInsideIterator */
    public ExtentInsideIterator(Parameters parameters,
            ExtentIterator innerIterator,
            ExtentIterator outerIterator) throws IOException {
        super(new ExtentIterator[] { innerIterator, outerIterator });
        this.innerIterator = innerIterator;
        this.outerIterator = outerIterator;
        findDocument();
    }

    public void loadExtents() {
        ExtentArrayIterator inner = new ExtentArrayIterator(innerIterator.extents());
        ExtentArrayIterator outer = new ExtentArrayIterator(outerIterator.extents());

        while (!inner.isDone() && !outer.isDone()) {
            if (outer.current().contains(inner.current())) {
                extents.add(inner.current());
                inner.next();
            } else if (outer.current().end <= inner.current().begin) {
                outer.next();
            } else {
                inner.next();
            }
        }
    }
}
