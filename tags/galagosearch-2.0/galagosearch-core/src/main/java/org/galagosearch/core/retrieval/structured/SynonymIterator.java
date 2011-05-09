// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.util.ArrayList;
import java.util.PriorityQueue;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class SynonymIterator extends ExtentDisjunctionIterator {
    public SynonymIterator(Parameters parameters, ExtentIterator[] iterators) {
        super(iterators);
        loadExtents();
    }

    public void loadExtents() {
        if (iterators.size() == 0) return;
        ExtentIterator iter = iterators.poll();
        document = iter.document();

        // get all the iteators that point to this document
        ArrayList<ExtentIterator> useable = new ArrayList<ExtentIterator>();
        while (iterators.size() > 0 && iterators.peek().document() == document) {
            useable.add(iterators.poll());
        }
        useable.add(iter);

        // make a priority queue of these ExtentArrayIterators
        PriorityQueue<ExtentArrayIterator> arrayIterators = new PriorityQueue<ExtentArrayIterator>();
        for (ExtentIterator iterator : useable) {
            arrayIterators.offer(new ExtentArrayIterator(iterator.extents()));
        }
        while (arrayIterators.size() > 0) {
            ExtentArrayIterator top = arrayIterators.poll();
            extents.add(top.current());

            if (top.next()) {
                arrayIterators.offer(top);
            }
        }

        // put back the ones we used
        for (ExtentIterator i : useable) {
            if (!i.isDone()) {
                iterators.offer(i);
            }
        }
    }
}
