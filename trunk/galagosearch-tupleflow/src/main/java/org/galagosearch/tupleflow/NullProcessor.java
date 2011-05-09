// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.tupleflow;

import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 *
 * @author trevor
 */

public class NullProcessor<T> implements Processor<T> {
    Class<T> inputClass;
    
    public NullProcessor() {
        inputClass = null;
    }
    
    public NullProcessor(TupleFlowParameters parameters) throws ClassNotFoundException {
        String className = parameters.getXML().get("class");
        this.inputClass = (Class<T>) Class.forName(className);
    }
    
    public NullProcessor(Class<T> inputClass) { this.inputClass = inputClass; }
    public void process(T object) {}
    public void close() {}
     
    public static String getInputClass(TupleFlowParameters parameters) {
        return parameters.getXML().get("class", "");
    }
   
    public static String[] getInputOrder(TupleFlowParameters parameters) {
        String[] orderSpec = parameters.getXML().get("order", "").split(" ");
        return orderSpec;
    }
    
    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        Verification.requireParameters(new String[] { "class" }, parameters.getXML(), handler);
    }
}
