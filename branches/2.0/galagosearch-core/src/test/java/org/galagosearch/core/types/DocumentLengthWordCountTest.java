package org.galagosearch.core.types;

import junit.framework.*;
import org.galagosearch.tupleflow.Order;

/**
 *
 * @author trevor
 */
public class DocumentLengthWordCountTest extends TestCase {
    public DocumentLengthWordCountTest(String testName) {
        super(testName);
    }

    public void testToString() {
        DocumentLengthWordCount instance = new DocumentLengthWordCount();
        String result = instance.toString();
        assertEquals(result, "null,0,null,0");

        instance = new DocumentLengthWordCount("hey", 2, "you", 5);
        result = instance.toString();
        assertEquals("hey,2,you,5", result);
    }

    public void testGetOrder() {
        String spec = "";
        DocumentLengthWordCount instance = new DocumentLengthWordCount();

        Order<DocumentLengthWordCount> expResult = null;
        Order<DocumentLengthWordCount> result = instance.getOrder(spec);
        assertEquals(expResult, result);

        spec = "+document";
        result = instance.getOrder(spec);
        assertNotNull(result);
    }
}
