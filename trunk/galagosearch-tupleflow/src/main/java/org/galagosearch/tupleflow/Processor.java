// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.tupleflow;

import java.io.IOException;

public interface Processor<T> extends Step {
    public void process(T object) throws IOException;
    public void close() throws IOException;
}
