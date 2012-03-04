// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

/**
 *
 * @author trevor
 */
public class Extent {
    public double weight;
    public int document;
    public int begin;
    public int end;

    public int compareTo(Extent other) {
        return other.document - document;
    }

    public boolean contains(Extent other) {
        return begin <= other.begin && end >= other.end;
    }
}
