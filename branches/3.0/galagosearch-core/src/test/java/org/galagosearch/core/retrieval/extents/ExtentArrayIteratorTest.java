
package org.galagosearch.core.retrieval.extents;

import org.galagosearch.core.retrieval.structured.ExtentArrayIterator;
import org.galagosearch.core.util.ExtentArray;
import junit.framework.*;

/**
 *
 * @author trevor
 */
public class ExtentArrayIteratorTest extends TestCase {
    
    public ExtentArrayIteratorTest(String testName) {
        super(testName);
    }

    public void testEmpty() {
        ExtentArrayIterator instance = new ExtentArrayIterator( new ExtentArray() );
        assertTrue( instance.isDone() );
    }

    public void testSingleExtent() {
        ExtentArray array = new ExtentArray();
        array.add( 1, 5, 7 );
        
        ExtentArrayIterator instance = new ExtentArrayIterator( array );
        assertFalse( instance.isDone() );
        assertEquals( instance.current().document, 1 );
        assertEquals( instance.current().begin, 5 );
        assertEquals( instance.current().end, 7 );
        
        instance.next();
        assertTrue( instance.isDone() );
    }

    public void testTwoExtents() {
        ExtentArray array = new ExtentArray();
        array.add( 1, 5, 7 );
        array.add( 1, 9, 11 );
        
        ExtentArrayIterator instance = new ExtentArrayIterator( array );
        assertFalse( instance.isDone() );
        assertEquals( instance.current().document, 1 );
        assertEquals( instance.current().begin, 5 );
        assertEquals( instance.current().end, 7 );

        instance.next();
        assertFalse( instance.isDone() );
        assertEquals( instance.current().document, 1 );
        assertEquals( instance.current().begin, 9 );
        assertEquals( instance.current().end, 11 );

        instance.next();
        assertTrue( instance.isDone() );
    }
}
