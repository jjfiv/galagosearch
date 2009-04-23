// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.tupleflow;

/**
 * An object that can generate objects of type T
 * @author trevor
 */
public interface Source<T> extends Step {
    public void setProcessor(Step processor) throws IncompatibleProcessorException;
}
