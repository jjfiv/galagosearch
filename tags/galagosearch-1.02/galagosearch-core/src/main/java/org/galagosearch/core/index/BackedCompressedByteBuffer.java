// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import org.galagosearch.tupleflow.Utility;

/**
 * A BackedCompressedByteBuffer is like a CompressedByteBuffer,
 * but it can overflow into disk storage if necessary.  Unlike a 
 * CompressedByteBuffer, there's no getBytes() method since that 
 * wouldn't makes sense if all the data is on disk.
 *
 * @author trevor
 */
public class BackedCompressedByteBuffer {
    ArrayList<File> segments;
    CompressedByteBuffer buffer;
    long diskLength;
    long threshold;

    /** Creates a new instance of BackedCompressedByteBuffer */
    public BackedCompressedByteBuffer(long threshold) {
        buffer = new CompressedByteBuffer();
        segments = new ArrayList<File>();
        this.threshold = threshold;
    }

    public BackedCompressedByteBuffer() {
        this(1024 * 1024);
    }

    public void add(long value) throws IOException {
        buffer.add(value);

        if (buffer.length() > threshold) {
            flush();
        }
    }

    public void add(CompressedByteBuffer other) throws IOException {
        if (other.length() > threshold) {
            flush();
            flushBuffer(other);
        } else {
            buffer.add(other);
        }
    }

    public void addFloat(float f) throws IOException {
        buffer.addFloat(f);

        if (buffer.length() > threshold) {
            flush();
        }
    }

    public void addRaw(int b) throws IOException {
        buffer.addRaw(b);

        if (buffer.length() > threshold) {
            flush();
        }
    }

    public void write(OutputStream stream) throws IOException {
        for (File f : segments) {
            Utility.copyFileToStream(f, stream);
        }

        buffer.write(stream);
    }

    public void flush() throws IOException {
        flushBuffer(buffer);
        buffer.clear();
    }

    void flushBuffer(CompressedByteBuffer other) throws IOException {
        File file = Utility.createTemporary();
        FileOutputStream stream = new FileOutputStream(file);
        other.write(stream);
        stream.close();
        diskLength += buffer.length();
        segments.add(file);
    }

    public long length() {
        return diskLength + buffer.length();
    }

    public void clear() {
        for (File f : segments) {
            f.delete();
        }
        segments.clear();
        buffer.clear();
        diskLength = 0;
    }

    public BufferInputStream getInputStream() throws IOException {
        return new BufferInputStream();
    }

    public class BufferInputStream extends InputStream {
        InputStream current = null;
        int fileSegment = -1;

        BufferInputStream() throws IOException {
            this.nextStream();
        }

        private boolean nextStream() throws IOException {
            if (fileSegment < segments.size() - 1) {
                if (current != null) {
                    current.close();
                }
                fileSegment++;
                current = new FileInputStream(segments.get(fileSegment));
                return true;
            } else if (fileSegment == segments.size() - 1) {
                current = new ByteArrayInputStream(buffer.getBytes(), 0, buffer.length());
                fileSegment++;
                return true;
            } else {
                current = null;
                return false;
            }
        }

        @Override
        public int available() throws IOException {
            if (current == null) {
                return 0;
            }
            if (current instanceof ByteArrayInputStream) {
                return current.available();
            }
            long total = 0;
            for (int i = fileSegment + 1; i < segments.size(); i++) {
                File f = segments.get(i);
                total += f.length();
            }

            total += buffer.length();
            total += current.available();

            if (total > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
            return (int) total;
        }

        @Override
        public void close() throws IOException {
            if (current != null) {
                current.close();
            }
        }

        @Override
        public int read(byte[] arg) throws IOException {
            return read(arg, 0, arg.length);
        }

        @Override
        public int read(byte[] arg, int offset, int length) throws IOException {
            if (current == null) {
                return -1;
            }
            int result = current.read(arg, offset, length);
            int total = 0;

            while (total < length) {
                if (result >= 0) {
                    total += result;
                }

                if (nextStream() == false) {
                    if (total > 0) {
                        return total;
                    } else {
                        return result;
                    }
                }

                result = current.read(arg, offset + total, length - total);
            }

            return total;
        }

        public int read() throws IOException {
            if (current == null) {
                return -1;
            }
            int result = current.read();

            while (result < 0 && nextStream()) {
                result = current.read();
            }
            return result;
        }
    }
}
