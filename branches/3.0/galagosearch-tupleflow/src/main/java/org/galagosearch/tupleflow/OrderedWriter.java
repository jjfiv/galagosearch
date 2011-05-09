// BSD License (http://galagosearch.org/license)

package org.galagosearch.tupleflow;

import java.io.IOException;

public abstract class OrderedWriter<T> implements Processor<T> {
    public abstract void process(T object) throws IOException;
}
