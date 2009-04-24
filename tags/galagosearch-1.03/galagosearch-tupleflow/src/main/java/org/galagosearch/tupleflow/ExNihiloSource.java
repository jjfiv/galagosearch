// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow;

import java.io.IOException;

/**
 *
 * @author trevor
 */
public interface ExNihiloSource<T> extends Source<T> {
    public void run() throws IOException;
}
