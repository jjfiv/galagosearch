/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.tupleflow;

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
    
    public void testAdd() {
        Parameters p = new Parameters();
        p.add("one", "1");
        p.add("two", "2");
        
        assertTrue(p.get("one").equals("1"));
        assertTrue(p.get("two").equals("2"));
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
