// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow.execution;

import java.util.ArrayList;

/**
 * Represents a multi block in a TupleFlow stage.  Its children 
 * must be lists of TupleFlow steps.
 * 
 * @author trevor
 */
public class MultiStep extends Step {
    public ArrayList<ArrayList<Step>> groups = new ArrayList<ArrayList<Step>>();

    public MultiStep() {
    }
}
