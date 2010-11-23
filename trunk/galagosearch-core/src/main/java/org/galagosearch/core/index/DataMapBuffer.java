// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * a buffer that keeps track of the number of
 * bytes written to file for data indexing purposes
 * It uses a CompressedByteBuffer to apply a vbyte compression scheme
 * 
 * Assumptions; files written are not longer than Long.MAX_VALUE bytes
 * 
 * @author sjh
 */
public class DataMapBuffer {
  DataOutputStream output;
  CompressedByteBuffer buffer;
  long diskLength;
  int threshold;

  public DataMapBuffer(DataOutputStream output, int threshold) {
    this.output = output;
    buffer = new CompressedByteBuffer();
    this.threshold = threshold;
  }

  public void add(long value) throws IOException {
    buffer.add(value);

    if (buffer.length() > threshold) {
      flush();
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

  public void addBytes(byte[] data) throws IOException {
    for(byte b : data){
      buffer.addRaw(b);
    }
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
      if (buffer.length() > threshold) {
        flush();
      }
    }
  }

  public void flush() throws IOException {
    flushBuffer(buffer);
    buffer.clear();
  }

  void flushBuffer(CompressedByteBuffer other) throws IOException {
    other.write(output);
    diskLength += buffer.length();
  }

  public long length() {
    return diskLength + buffer.length();
  }

  public void close() throws IOException{
    flush();
    output.close();
  }
}
