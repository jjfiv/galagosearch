// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow.execution;

/**
 *
 * @author trevor
 */
public interface ErrorHandler {
    public void addError(String errorString);
    public void addWarning(String warningString);
}
