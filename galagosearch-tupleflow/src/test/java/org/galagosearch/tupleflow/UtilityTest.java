/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.tupleflow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import junit.framework.TestCase;

/**
 *
 * @author trevor
 */
public class UtilityTest extends TestCase {
    
    public UtilityTest(String testName) {
        super(testName);
    }

    public void testCopyStream() throws IOException {
        byte[] data = { 0, 1, 2, 3, 4, 5 };
        ByteArrayInputStream input = new ByteArrayInputStream(data);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        Utility.copyStream(input, output);
        byte[] result = output.toByteArray();
        assertEquals(0, Utility.compare(data, result));
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

    public void testMakeParentDirectories() throws IOException {
        // This gives us a usable temporary path.
        File f = Utility.createTemporary();

        String parent = f.getParent() + File.separator +
                        Utility.join(new String[]{"bbb", "b", "c", "d"}, File.separator);
        String path = parent + File.separator + "e";
        Utility.makeParentDirectories(path);

        // The parent directory should exist
        assertTrue(new File(parent).isDirectory());
        // but the file itself should not exist.
        assertFalse(new File(path).exists());

        Utility.deleteDirectory(new File(f.getParent() + File.separator + "bbb"));
        f.delete();
    }
}
