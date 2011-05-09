// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.tupleflow;

import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;
import java.io.IOException;

/**
 *
 * @author trevor
 */
public class NullSource<T> implements ExNihiloSource<T> {
    public Processor<T> processor;
    Class<T> outputClass;

    public NullSource(TupleFlowParameters parameters) throws ClassNotFoundException {
        String className = parameters.getXML().get("class");
        this.outputClass = (Class<T>) Class.forName(className);
    }

    public NullSource(Class<T> outputClass) {
        this.outputClass = outputClass;
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        Verification.requireParameters(new String[]{"class"}, parameters.getXML(), handler);
        Verification.requireClass(parameters.getXML().get("class"), handler);
    }

    public void setProcessor(Step processor) throws IncompatibleProcessorException {
        Linkage.link(this, processor);
    }

    public void run() throws IOException {
        processor.close();
    }

    public static String getOutputClass(TupleFlowParameters parameters) {
        return parameters.getXML().get("class", "");
    }
}
