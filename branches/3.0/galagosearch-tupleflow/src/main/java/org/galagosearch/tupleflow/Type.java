// BSD License (http://galagosearch.org)

package org.galagosearch.tupleflow;

/**
 *
 * @author trevor
 */
public interface Type<T> {
    public Order<T> getOrder(String... fields);
}
