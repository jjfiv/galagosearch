// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow;

import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author trevor
 */
public class VByteOutput implements DataOutput {
    DataOutput output;

    /** Creates a new instance of VByteOutput */
    public VByteOutput(DataOutput output) {
        this.output = output;
    }

    public void writeUTF(String string) throws IOException {
        writeInt(string.length());
        output.writeUTF(string);
    }

    public void writeBytes(String string) throws IOException {
        writeInt(string.length());
        output.writeBytes(string);
    }

    public void writeChars(String string) throws IOException {
        writeInt(string.length());
        output.writeChars(string);
    }
    
    public void writeString(String string) throws IOException {
        byte[] bytes = Utility.makeBytes(string);
        writeInt(bytes.length);
        write(bytes);
    }

    public void write(byte[] b, int i, int i0) throws IOException {
        output.write(b, i, i0);
    }

    public void write(byte[] b) throws IOException {
        output.write(b);
    }

    public void write(int i) throws IOException {
        assert i >= 0;

        if (i < 1 << 7) {
            output.writeByte((i | 0x80));
        } else if (i < 1 << 14) {
            output.writeByte((i >> 0) & 0x7f);
            output.writeByte(((i >> 7) & 0x7f) | 0x80);
        } else if (i < 1 << 21) {
            output.writeByte((i >> 0) & 0x7f);
            output.writeByte((i >> 7) & 0x7f);
            output.writeByte(((i >> 14) & 0x7f) | 0x80);
        } else if (i < 1 << 28) {
            output.writeByte((i >> 0) & 0x7f);
            output.writeByte((i >> 7) & 0x7f);
            output.writeByte((i >> 14) & 0x7f);
            output.writeByte(((i >> 21) & 0x7f) | 0x80);
        } else {
            output.writeByte((i >> 0) & 0x7f);
            output.writeByte((i >> 7) & 0x7f);
            output.writeByte((i >> 14) & 0x7f);
            output.writeByte((i >> 21) & 0x7f);
            output.writeByte(((i >> 28) & 0x7f) | 0x80);
        }
    }

    public void writeByte(int i) throws IOException {
        write(i);
    }

    public void writeChar(int i) throws IOException {
        write(i);
    }

    public void writeInt(int i) throws IOException {
        write(i);
    }

    public void writeShort(int i) throws IOException {
        write(i);
    }

    public void writeBoolean(boolean b) throws IOException {
        if (b) {
            write(1);
        } else {
            write(0);
        }
    }

    public void writeLong(long i) throws IOException {
        assert i >= 0;

        if (i < 1 << 7) {
            output.writeByte((int) (i | 0x80));
        } else if (i < 1 << 14) {
            output.writeByte((int) (i >> 0) & 0x7f);
            output.writeByte((int) ((i >> 7) & 0x7f) | 0x80);
        } else if (i < 1 << 21) {
            output.writeByte((int) (i >> 0) & 0x7f);
            output.writeByte((int) (i >> 7) & 0x7f);
            output.writeByte((int) ((i >> 14) & 0x7f) | 0x80);
        } else {
            while (i >= 1 << 7) {
                output.writeByte((int) (i & 0x7f));
                i >>= 7;
            }

            output.writeByte((int) (i | 0x80));
        }
    }

    public void writeDouble(double d) throws IOException {
        output.writeLong(Double.doubleToRawLongBits(d));
    }

    public void writeFloat(float f) throws IOException {
        output.writeInt(Float.floatToRawIntBits(f));
    }
}
