// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow;

import java.io.IOException;

/**
 *
 * @author trevor
 */
public interface TypeReader<T> extends ExNihiloSource<T> {
    T read() throws IOException;

    void run() throws IOException;
}
