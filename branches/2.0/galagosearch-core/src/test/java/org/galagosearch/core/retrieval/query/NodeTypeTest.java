/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.query;

import org.galagosearch.core.retrieval.query.NodeType;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Date;
import junit.framework.TestCase;
import org.galagosearch.core.retrieval.structured.ExtentIterator;
import org.galagosearch.core.retrieval.structured.StructuredIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class NodeTypeTest extends TestCase {
    
    public NodeTypeTest(String testName) {
        super(testName);
    }

    public void testGetIteratorClass() {
        NodeType n = new NodeType(ExtentIterator.class);
        assertEquals(ExtentIterator.class, n.getIteratorClass());
    }
    
    public void testIsStructuredIteratorOrArray() {
        NodeType n = new NodeType(ExtentIterator.class);
        assertTrue(n.isStructuredIteratorOrArray(ExtentIterator.class));
        assertTrue(n.isStructuredIteratorOrArray(StructuredIterator.class));
        assertFalse(n.isStructuredIteratorOrArray(Integer.class));
        assertFalse(n.isStructuredIteratorOrArray(Date.class));
        assertTrue(n.isStructuredIteratorOrArray(new ExtentIterator[0].getClass()));
    }

    public static class FakeIterator implements StructuredIterator {
        public FakeIterator(Parameters parameters, ExtentIterator one, StructuredIterator[] two) {
        }

        public void reset() throws IOException {
        }
    }
    
    public void testGetInputs() throws Exception {
        NodeType n = new NodeType(FakeIterator.class);
        Class[] input = n.getInputs();
        assertEquals(3, input.length);
        assertEquals(Parameters.class, input[0]);
        assertEquals(ExtentIterator.class, input[1]);
        assertEquals(new StructuredIterator[0].getClass(), input[2]);
    }
    
    public void testGetParameterTypes() throws Exception {
        NodeType n = new NodeType(FakeIterator.class);
        Class[] input = n.getParameterTypes(5);
        assertEquals(5, input.length);
        assertEquals(Parameters.class, input[0]);
        assertEquals(ExtentIterator.class, input[1]);
        assertEquals(StructuredIterator.class, input[2]);
        assertEquals(StructuredIterator.class, input[3]);
        assertEquals(StructuredIterator.class, input[4]);
    }
    
    public void testGetConstructor() throws Exception {
        NodeType n = new NodeType(FakeIterator.class);
        Constructor c = n.getConstructor();
        Constructor actual =
                FakeIterator.class.getConstructor(Parameters.class, ExtentIterator.class,
                                                  new StructuredIterator[0].getClass());
        assertEquals(actual, c);
    }
}
