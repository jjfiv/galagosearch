// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.OutputStream;

/**
 *
 * @author trevor
 */
public interface IndexElement {
    public byte[] key();
    public long dataLength();
    public void write(final OutputStream stream) throws java.io.IOException;
}
