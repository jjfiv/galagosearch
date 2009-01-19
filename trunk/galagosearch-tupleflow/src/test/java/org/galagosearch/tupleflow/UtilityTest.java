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
public class UtilityTest extends TestCase {
    
    public UtilityTest(String testName) {
        super(testName);
    }

    public void testFilterFlags() {
        String[][] filtered;
        
        filtered = Utility.filterFlags(new String[] {});
        assertEquals(2, filtered.length);
        
        filtered = Utility.filterFlags(new String[] {"--flag", "notflag", "--another"});
        assertEquals(2, filtered.length);
        
        String[] flags = filtered[0];
        String[] nonFlags = filtered[1];

        assertEquals(2, flags.length);
        assertEquals("--flag", flags[0]);
        assertEquals("--another", flags[1]);

        assertEquals(1, nonFlags.length);
        assertEquals("notflag", nonFlags[0]);
    }
}
