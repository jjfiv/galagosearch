// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import org.galagosearch.core.util.ExtentArray;

/**
 * This extends a identifier-ordered navigable count iterator by returning
 * arrays of extents, each of which is a position range (start - end), docid, and
 * weight.
 * 
 * @author trevor, irmarc
 */
public interface ExtentIterator extends DataIterator<ExtentArray> {
    public ExtentArray extents();
}
