/*
 * BackedCompressedByteBufferTest.java
 * JUnit based test
 *
 * Created on October 8, 2007, 2:22 PM
 */
package org.galagosearch.core.index;

import org.galagosearch.core.index.BackedCompressedByteBuffer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import junit.framework.TestCase;

/**
 *
 * @author trevor
 */
public class BackedCompressedByteBufferTest extends TestCase {
    BackedCompressedByteBuffer instance = null;

    public BackedCompressedByteBufferTest(String testName) {
        super(testName);
    }

    @Override
    public void tearDown() {
        if (instance != null) {
            instance.clear();
        }
    }

    public void testAdd() throws Exception {
        instance = new BackedCompressedByteBuffer();
        instance.add(5);
        instance.add(10);
        instance.add(200);
        instance.add(400);

        InputStream stream = instance.getInputStream();

        assertEquals(stream.read(), 5 | 1 << 7);
        assertEquals(stream.read(), 10 | 1 << 7);
        assertEquals(stream.read(), 72);
        assertEquals(stream.read(), 1 | 1 << 7);
        assertEquals(stream.read(), 16);
        assertEquals(stream.read(), 3 | 1 << 7);

        assertFalse(stream.available() > 0);
        stream.close();
    }

    public void testAddFloat() throws Exception {
        float f = 1.0F;
        instance = new BackedCompressedByteBuffer();
        instance.addFloat(f);

        assertEquals(4, instance.length());
        byte[] result = new byte[4];
        instance.getInputStream().read(result);
        int floatBits = Float.floatToIntBits(f);

        assertEquals(result[0], (byte) (floatBits >> 24));
        assertEquals(result[1], (byte) (floatBits >> 16));
        assertEquals(result[2], (byte) (floatBits >> 8));
        assertEquals(result[3], (byte) (floatBits >> 0));
    }

    public void testAddRaw() throws Exception {
        int b = 1;
        instance = new BackedCompressedByteBuffer();
        instance.addRaw(b);

        InputStream s = instance.getInputStream();
        assertEquals(b, s.read());
        assertTrue(s.available() == 0);
    }

    public void testFlush() throws Exception {
        instance = new BackedCompressedByteBuffer();

        instance.addRaw(5);
        instance.flush();
        instance.addRaw(6);
        instance.addRaw(7);
        instance.flush();
        instance.addRaw(8);
        instance.flush();
        instance.addRaw(9);

        assertEquals(5, instance.length());

        InputStream i = instance.getInputStream();

        assertEquals(5, i.available());
        byte[] result = new byte[5];
        i.read(result);

        byte[] expected = new byte[]{5, 6, 7, 8, 9};
        assertTrue(Arrays.equals(result, expected));
    }

    public void testWrite() throws Exception {
        instance = new BackedCompressedByteBuffer();

        instance.addRaw(5);
        instance.flush();
        instance.addRaw(6);
        instance.addRaw(7);
        instance.flush();
        instance.addRaw(8);
        instance.flush();
        instance.addRaw(9);

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        instance.write(stream);
        byte[] bytes = stream.toByteArray();
        byte[] expected = new byte[]{5, 6, 7, 8, 9};

        assertTrue(Arrays.equals(bytes, expected));
    }

    public void testLength() throws IOException {
        instance = new BackedCompressedByteBuffer();

        instance.addRaw(4);
        assertEquals(instance.length(), 1);
        instance.addRaw(5);
        assertEquals(instance.length(), 2);
        instance.clear();
    }

    public void testClear() throws IOException {
        instance = new BackedCompressedByteBuffer();

        instance.addRaw(4);
        assertEquals(instance.length(), 1);
        instance.addRaw(5);
        assertEquals(instance.length(), 2);
        instance.clear();
        assertEquals(instance.length(), 0);
    }

    public void testGetInputStream() throws IOException {
        instance = new BackedCompressedByteBuffer();
        instance.addRaw(5);

        InputStream i = instance.getInputStream();
        byte[] b = new byte[1];
        i.read(b);
        assertEquals(b[0], 5);

        int result = i.read();
        assertTrue(result < 0);
    }
}
