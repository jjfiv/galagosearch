// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.tupleflow.execution;

import org.galagosearch.tupleflow.Parameters;
import java.io.Serializable;

/**
 *
 * @author trevor
 */
public class Step implements Serializable {
    protected FileLocation location;
    private String className;
    private String inputType;
    private String outputType;
    private String[] inputOrder;
    private String[] outputOrder;
    private Parameters parameters;

    public Step() {
    }

    public Step(Class c) {
        this(null, c.getName(), new Parameters());
    }

    public Step(String className) {
        this(null, className, new Parameters());
    }
    
    public Step(Class c, Parameters parameters) {
        this(null, c.getName(), parameters);
    }

    public Step(String className, Parameters parameters) {
        this(null, className, parameters);
    }

    public Step(FileLocation location, String className, Parameters parameters) {
        this.location = location;
        this.className = className;
        this.parameters = parameters;
    }

    public FileLocation getLocation() {
        return location;
    }

    public String getClassName() {
        return className;
    }

    public Parameters getParameters() {
        return parameters;
    }

    public boolean isStepClassAvailable() {
        return Verification.isClassAvailable(className);
    }

    @Override
    public String toString() {
        return className;
    }
}
