// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow;

/**
 *
 * @author trevor
 */
public class IncompatibleProcessorException extends Exception {
    public IncompatibleProcessorException(String message) {
        super(message);
    }

    public IncompatibleProcessorException(String message, Throwable e) {
        super(message, e);
    }
}
