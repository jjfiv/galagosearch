// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.util;

import java.util.Arrays;
import org.galagosearch.tupleflow.Utility;

/**
 * Boxing a byte[] object to ensure proper operation
 *
 * @author irmarc
 */
public class Bytes {
  byte[] bytes;

  public Bytes(byte[] b) {
    bytes = Arrays.copyOf(b, b.length);
  }

  public boolean equals(Bytes that) {
    return Arrays.equals(this.bytes, that.bytes);
  }

  public byte[] getBytes() {
    return bytes;
  }

  public int hashCode() {
    return Arrays.hashCode(this.bytes);
  }

  public int compareTo(Bytes that) {
    return Utility.compare(this.bytes, that.bytes);
  }
}
