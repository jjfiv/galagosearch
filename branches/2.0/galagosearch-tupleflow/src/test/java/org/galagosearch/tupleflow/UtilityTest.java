/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.tupleflow;

import java.io.*;
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

    public void testConverters() throws IOException {
	// String
	String testString = "I am a little teapot, short and stout";
	byte[] buffer = Utility.fromString(testString);
	String convString = Utility.toString(buffer);
	assertTrue(testString.equals(convString));

	// short
	short testShort = 2345;
	buffer = Utility.fromShort(testShort);
	short convShort = Utility.toShort(buffer);
	assertEquals(testShort, convShort);
	
	// int
	int testInt = 832905257;
	buffer = Utility.fromInt(testInt);
	int convInt = Utility.toInt(buffer);
	assertEquals(testInt, convInt);

	// long
	long testLong = 9034790013458L;
	buffer = Utility.fromLong(testLong);
	long convLong = Utility.toLong(buffer);
	assertEquals(testLong, convLong);

	// compression
	DataOutputStream out;
	ByteArrayOutputStream bout;
	DataInputStream in;
	ByteArrayInputStream bin;
	bout = new ByteArrayOutputStream();
	out = new DataOutputStream(bout);
	
	// int on data streams
	testInt = 313;
	Utility.compressInt(out, testInt);
	bin = new ByteArrayInputStream(bout.toByteArray());
	in = new DataInputStream(bin);
	convInt = Utility.uncompressInt(in);
	assertEquals(testInt, convInt);

	// int uncompressed from byte array
	buffer = bout.toByteArray();
	convInt = Utility.uncompressInt(buffer, 0);
	assertEquals(testInt, convInt);

	// long on data streams		      
	bout.reset();
	testLong = 453279;
	Utility.compressLong(out, testLong);
	bin = new ByteArrayInputStream(bout.toByteArray());
	in = new DataInputStream(bin);
	convLong = Utility.uncompressLong(in);
	assertEquals(testLong, convLong);

	// long uncompressed from byte array
	buffer = bout.toByteArray();
	convLong = Utility.uncompressLong(buffer, 0);
	assertEquals(testLong, convLong);
    }
}
