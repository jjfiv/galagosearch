// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Stores lists of integers in vbyte compressed form.  This
 * is useful for buffering data that will be stored
 * compressed on disk.
 */
public class CompressedByteBuffer {
    byte[] values;
    int position;

    public CompressedByteBuffer() {
        clear();
    }

    /**
     * Add a single byte to the buffer.  This byte is written
     * directly to the buffer without compression.
     *
     * @param value The byte value to add.
     */
    public void addRaw(int value) {
        if (position >= values.length) {
            byte[] nValues = new byte[values.length * 2];
            System.arraycopy(values, 0, nValues, 0, values.length);
            values = nValues;
        }

        values[position] = (byte) value;
        position += 1;
    }

    /** 
     * Adds a single number to the buffer.  This number is 
     * converted to compressed form before it is stored.
     */
    public void add(long i) {
        if (i < 1 << 7) {
            addRaw((int) (i | 0x80));
        } else if (i < 1 << 14) {
            addRaw((int) (i >> 0) & 0x7f);
            addRaw((int) ((i >> 7) & 0x7f) | 0x80);
        } else if (i < 1 << 21) {
            addRaw((int) (i >> 0) & 0x7f);
            addRaw((int) (i >> 7) & 0x7f);
            addRaw((int) ((i >> 14) & 0x7f) | 0x80);
        } else {
            while (i >= 1 << 7) {
                addRaw((int) (i & 0x7f));
                i >>= 7;
            }

            addRaw((int) (i | 0x80));
        }
    }

    /**
     * Adds a floating point value, (4 bytes) to the buffer.
     * This is an uncompressed value.
     */
    public void addFloat(float value) {
        int bits = Float.floatToIntBits(value);

        addRaw((bits >>> 24) & 0xFF);
        addRaw((bits >>> 16) & 0xFF);
        addRaw((bits >>> 8) & 0xFF);
        addRaw(bits & 0xFF);
    }

    /**
     * Copies the entire contents of another compressed
     * buffer to the end of this one.
     *
     * @param other The buffer to copy.
     */
    public void add(CompressedByteBuffer other) {
        int totalLength = other.length() + length();
        byte[] newValues = new byte[totalLength];

        System.arraycopy(values, 0, newValues, 0, position);
        System.arraycopy(other.values, 0, newValues, position, other.position);
        values = newValues;
        position = totalLength;
    }

    /** 
     * Erases the contents of this buffer and sets its
     * length to zero.
     */
    public void clear() {
        values = new byte[16];
        position = 0;
    }

    /**
     * Returns a byte array containing the contents of this buffer.
     * The array returned may be larger than the actual length of
     * the stored data.  Use the length method to determine the 
     * true data length.
     */
    public byte[] getBytes() {
        return values;
    }

    /** 
     * Returns the length of the data stored in this buffer.
     */
    public int length() {
        return position;
    }

    /**
     * Writes the contents of this buffer to a stream.
     */
    public void write(OutputStream stream) throws IOException {
        stream.write(values, 0, position);
    }
}

