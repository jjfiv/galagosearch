// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow;

import java.io.DataInput;
import java.io.IOException;

/**
 *
 * @author trevor
 */
public class VByteInput implements DataInput {
    DataInput input;

    public VByteInput(DataInput input) {
        this.input = input;
    }

    public void readFully(byte[] b, int i, int i0) throws IOException {
        input.readFully(b, i, i0);
    }

    public void readFully(byte[] b) throws IOException {
        input.readFully(b);
    }
    
    public String readString() throws IOException {
        int length = readInt();
        byte[] data = new byte[length];
        input.readFully(data);
        return Utility.makeString(data);
    }

    public int skipBytes(int i) throws IOException {
        return input.skipBytes(i);
    }

    public int readUnsignedShort() throws IOException {
        int result = readInt();
        return (int) (result & 0xffff);
    }

    public boolean readBoolean() throws IOException {
        int result = readInt();
        return result != 0 ? true : false;
    }

    public byte readByte() throws IOException {
        int result = readInt();
        return (byte) (result & 0xff);
    }

    public char readChar() throws IOException {
        int result = readInt();
        return (char) (result & 0xffff);
    }

    public double readDouble() throws IOException {
        long result = input.readLong();
        return Double.longBitsToDouble(result);
    }

    public float readFloat() throws IOException {
        int result = input.readInt();
        return Float.intBitsToFloat(result);
    }

    public int readInt() throws IOException {
        int result = 0;
        int b;

        for (int position = 0; true; position++) {
            assert position < 6;
            b = input.readUnsignedByte();

            if ((b & 0x80) == 0x80) {
                result |= ((b & 0x7f) << (7 * position));
                break;
            } else {
                result |= (b << (7 * position));
            }
        }

        return result;
    }

    public long readLong() throws IOException {
        long result = 0;
        long b;

        for (int position = 0; true; position++) {
            b = input.readUnsignedByte();

            if ((b & 0x80) == 0x80) {
                result |= ((long) (b & 0x7f) << (7 * position));
                break;
            } else {
                result |= ((long) b << (7 * position));
            }
        }

        return result;
    }

    public String readLine() throws IOException {
        throw new IOException("Operation not supported.");
    }

    public short readShort() throws IOException {
        return (short) (readInt() & 0xffff);
    }

    public String readUTF() throws IOException {
        throw new IOException("Operation not supported.");
    }

    public int readUnsignedByte() throws IOException {
        return (readInt() & 0xff);
    }
}
