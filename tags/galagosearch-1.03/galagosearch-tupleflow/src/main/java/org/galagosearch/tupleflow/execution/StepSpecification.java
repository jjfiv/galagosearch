// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.tupleflow.execution;

import org.galagosearch.tupleflow.Order;
import org.galagosearch.tupleflow.Parameters;

/**
 * There are a few different things this interface needs to support.
 * 
 * <ul>
 * <li>Validation</li>
 * <li>Code completion</li>
 * </ul>
 * 
 * @author trevor
 */

/*
public interface TagSpecification {
    public String getName();
    public String getDocumentation();
    
    public boolean acceptsChildren();
    public boolean valueRequired();
    
    public List<TagSpecification> getChildren();
}

public interface ValueTagSpecification {
    public String getName();
    public String getDocumentation();
    
    public List<String> getCompletions();
    public boolean validate(String value, ErrorHandler handler);
}

public interface StructureTagSpecification {
    public String getName();
    public String getDocumentation();
    
    public List<TagSpecification> getChildren();
    public boolean validate(Parameters parameters, ErrorHandler handler);
}

public interface ParametersSpecification {
    // what top-level tags are supported? in what combinations?
    // rule: parent tag determines what child tags are supported.  
    // value of one child tag does not influence what other tags are valid,
    //      although it may influence value validation.
    public List<TagSpecification> getChildren();
    public boolean validate(Parameters parameters, ErrorHandler handler);
}
*/

public interface StepSpecification {
    public void setParameters(Parameters parameters);
    public void addErrors(ErrorHandler handler);
    
    public String getDocumentation();

    public Class getInput(String name);
    public Class getOutput(String name);
    public Order getInputOrder(String name);
    public Order getOutputOrder(String name);
    
    public boolean acceptsParameters();
    
    public Class getInputClass();
    public Class getOutputClass();
   
    public Order getInputOrder();
    public Order getOutputOrder();
}
