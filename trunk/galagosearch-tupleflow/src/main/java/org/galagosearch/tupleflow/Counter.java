// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.tupleflow;

/**
 * A counter sends statistics from individual TupleFlow workers back to
 * the JobExecutor.
 * 
 * @author trevor
 */
public interface Counter {
    void increment();
    void incrementBy(int value);
}
