/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.tupleflow;

import java.io.IOException;
import junit.framework.TestCase;

/**
 *
 * @author trevor
 */
public class ParametersTest extends TestCase {
    public ParametersTest(String testName) {
        super(testName);
    }

    public void testCreation() {
        Parameters p = new Parameters();
    }

    public void testParse() throws IOException {
        Parameters p = new Parameters();
        String xmlData = "<parameters><test>a</test></parameters>";
        p.parse(xmlData.getBytes());
        assertTrue(p.containsKey("test"));
        assertEquals("a", p.get("test"));
    }
    
    public void testAdd() {
        Parameters p = new Parameters();
        p.add("one", "1");
        p.add("two", "2");
        
        assertTrue(p.get("one").equals("1"));
        assertTrue(p.get("two").equals("2"));
    }

    public void testSet() {
        Parameters p = new Parameters();
        p.add("one", "1");
        p.add("one", "3");
        assertEquals(2, p.list("one").size());

        p.set("one", "2");
        assertEquals(1, p.list("one").size());
        assertTrue(p.get("one").equals("2"));
    }
    
    public void testContainsKey() {
        Parameters p = new Parameters();
        p.add("one", "1");
        assertTrue(p.containsKey("one"));
        assertFalse(p.containsKey("two"));
    }
    
    public void testGetLong() {
        Parameters p = new Parameters();
        p.add("one", "1");
        p.add("two", "2");

        assertEquals(1, p.get("one", 5));
        assertEquals(5, p.get("other", 5));
        assertTrue(p.get("one").equals("1"));
    }
    
    public void testGetBoolean() {
        Parameters p = new Parameters();
        p.add("one", "true");
        p.add("two", "2");
        assertTrue(p.get("one", false));
    }
}
