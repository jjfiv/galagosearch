// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public interface Reducer<T> {
    public ArrayList<T> reduce(List<T> input) throws IOException;
}
