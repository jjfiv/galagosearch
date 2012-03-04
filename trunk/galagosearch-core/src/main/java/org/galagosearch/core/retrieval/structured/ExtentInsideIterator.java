// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.tupleflow.Parameters;

/**
 * <p>Implements the #inside operator.  The #inside operator is usually implicit
 * in the query language, where <tt>a.b</tt> is equivalent to <tt>#inside(a b)</tt>.
 * This is usually used to find terms that occur in fields.  For example,
 * <tt>#1(bruce croft).author</tt>, which finds instances of "bruce croft" occurring
 * in the author field of a document.</p>
 *
 * @author trevor
 */
public class ExtentInsideIterator extends ExtentConjunctionIterator {
    ExtentIterator innerIterator;
    ExtentIterator outerIterator;

    /**
     * <p>Constructs an #inside instance.  For <tt>#inside(a b)</tt>, this
     * produces an extent whenever <tt>a</tt> is found inside <tt>b</tt>.</p>
     *
     * <p>For example, in the expression <tt>#inside(#1(white house) #extents:title())</tt>,
     * <tt>#1(white house)</tt> is the inner iterator and <tt>#extents:title()</tt>
     * is the outer iterator.  Whenever <tt>#1(white house)</tt> is found in the title of
     * a document, this is a match.  The extent for <tt>#1(white house)</tt> is returned
     * (not the extent for <tt>#extents:title()</tt> that surrounds it).</tt>
     *
     * @param parameters extra parameters, not used for anything.
     * @param innerIterator The source of extents that must be inside.
     * @param outerIterator The source of extents that must contain the inner extents.
     * @throws java.io.IOException
     */
    public ExtentInsideIterator(Parameters parameters,
            ExtentIterator innerIterator,
            ExtentIterator outerIterator) throws IOException {
        super(new ExtentIterator[] { innerIterator, outerIterator });
        this.innerIterator = innerIterator;
        this.outerIterator = outerIterator;
        findDocument();
    }

    /**
     * This method is called whenever the ExtentConjunctionIterator has verified
     * that both the inner and outer iterators match this document.  This method's job
     * is to find all matchin extents within the document, if they exist.
     */

    public void loadExtents() {
        ExtentArrayIterator inner = new ExtentArrayIterator(innerIterator.extents());
        ExtentArrayIterator outer = new ExtentArrayIterator(outerIterator.extents());

        while (!inner.isDone() && !outer.isDone()) {
            if (outer.current().contains(inner.current())) {
                extents.add(inner.current());
                inner.next();
            } else if (outer.current().end <= inner.current().begin) {
                outer.next();
            } else {
                inner.next();
            }
        }
    }
}
