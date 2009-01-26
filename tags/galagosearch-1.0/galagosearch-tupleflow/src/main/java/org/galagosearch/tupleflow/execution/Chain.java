// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow.execution;

import java.util.ArrayList;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Source;
import org.galagosearch.tupleflow.Step;

/**
 *
 * @author trevor
 */
public class Chain {
    ArrayList<Step> items = new ArrayList();

    public void add(Step stage) throws IncompatibleProcessorException {
        if (items.size() > 0) {
            // is this a ShreddedProcessor?
            Object previousSource = items.get(items.size() - 1);
            ((Source) previousSource).setProcessor(stage);
        }

        items.add(stage);
    }

    public Step getStage() {
        if (items.size() > 0) {
            Step first = items.get(0);
            return first;
        }
        return null;
    }
}
