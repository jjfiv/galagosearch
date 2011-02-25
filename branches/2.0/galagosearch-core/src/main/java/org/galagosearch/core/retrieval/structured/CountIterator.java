// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import org.galagosearch.core.index.ValueIterator;

/**
 * This is base interface for all inverted lists that return count information.
 * 12/11/2010 (irmarc): Refactored
 * 2/18/2011 (irmarc): Refactored again for Galago 2.0
 *
 * @see PositionIndexIterator
 * @author trevor, irmarc
 */
public interface CountIterator extends StructuredIterator, Comparable<CountIterator> {

    public int currentIdentifier();

    /**
     * Returns the number of occurrences of this iterator's term in
     * the current identifier.
     */
    public int count();
}
