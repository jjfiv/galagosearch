// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 * Takes two inputs, keys and values.  Each item in the values list that matches
 * a key is sent to the next stage.  Items are assumed to be sorted in order 
 * by "order".  That order also determines equality.
 * 
 * @author trevor
 */
public class Select<T> implements ExNihiloSource<T> {
    TypeReader<T> keys;
    TypeReader<T> values;
    public Processor<T> processor;
    Class<T> klass;
    Order<T> order;

    public Select(TupleFlowParameters parameters) throws IOException, ClassNotFoundException,
            NoSuchMethodException, IllegalArgumentException, InvocationTargetException,
            InstantiationException, IllegalAccessException {
        keys = parameters.getTypeReader("keys");
        values = parameters.getTypeReader("values");
        klass = (Class<T>) Class.forName(parameters.getXML().get("class"));
        String orderSpec = parameters.getXML().get("order");

        org.galagosearch.tupleflow.Type<T> type = (org.galagosearch.tupleflow.Type<T>) klass.
                getConstructor().newInstance();
        order = type.getOrder(orderSpec);
    }

    public void run() throws IOException {
        T key = keys.read();
        T value = values.read();
        Comparator<T> lessThan = order.lessThan();

        while (key != null && value != null) {
            int compare = lessThan.compare(key, value);

            if (compare < 0) {
                key = keys.read();
            } else if (compare > 0) {
                value = values.read();
            } else {
                processor.process(value);
                value = values.read();
            }
        }

        while (key != null) {
            key = keys.read();
        }
        while (value != null) {
            value = values.read();
        }
    }

    public Class<T> getOutputClass(TupleFlowParameters parameters) throws ClassNotFoundException {
        return (Class<T>) Class.forName(parameters.getXML().get("class"));
    }

    public void setProcessor(Step next) throws IncompatibleProcessorException {
        Linkage.link(this, next);
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) throws ClassNotFoundException {
        if (!Verification.requireParameters(new String[]{"class", "order"}, parameters.getXML(),
                                            handler)) {
            return;
        }
        String className = parameters.getXML().get("class");
        String[] order = parameters.getXML().get("order").split(" ");
        boolean result;

        result = Verification.requireClass(className, handler);
        result = result && Verification.requireOrder(className, order, handler);

        if (!result) {
            return;
        }
        Class klass = Class.forName(className);
        Verification.verifyTypeReader("keys", klass, order, parameters, handler);
        Verification.verifyTypeReader("values", klass, order, parameters, handler);
    }
}
