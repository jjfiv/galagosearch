// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow.execution;

/**
 * Represents an input step in a TupleFlow stage.
 * 
 * @author trevor
 */
public class InputStep extends Step {
    String id;

    public InputStep(String id) {
        this.id = id;
    }

    public InputStep(FileLocation location, String id) {
        this.id = id;
        this.location = location;
    }

    public String getId() {
        return id;
    }
}
