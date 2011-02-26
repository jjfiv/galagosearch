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
    public SynonymIterator(Parameters parameters, ExtentValueIterator[] iterators) {
        super(iterators);
        loadExtents();
    }

    public void loadExtents() {
        if (iterators.size() == 0) return;
        ExtentValueIterator iter = iterators.poll();
        document = iter.currentIdentifier();

        // get all the iteators that point to this intID
        ArrayList<ExtentValueIterator> useable = new ArrayList<ExtentValueIterator>();
        while (iterators.size() > 0 && iterators.peek().currentIdentifier() == document) {
            useable.add(iterators.poll());
        }
        useable.add(iter);

        // make a priority queue of these ExtentArrayIterators
        PriorityQueue<ExtentArrayIterator> arrayIterators = new PriorityQueue<ExtentArrayIterator>();
        for (ExtentValueIterator iterator : useable) {
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
        for (ExtentValueIterator i : useable) {
            if (!i.isDone()) {
                iterators.offer(i);
            }
        }
    }
}
