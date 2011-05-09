// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.mem;

import org.galagosearch.tupleflow.Parameters;
import java.io.IOException;

public class MemoryParameters extends Parameters {

    MemoryRetrieval r;

    public MemoryParameters(MemoryRetrieval r, Parameters p) {
        super();
        this.copy(p);
        this.r = r;
    }

    @Override
    public String get(String key) {
        try {
            if (key.equals("collectionLength") || key.equals("documentCount")) {
                if(containsKey(key))
                    return Integer.toString(Integer.parseInt(r.getRetrievalStatistics("").get(key)) + Integer.parseInt(super.get(key)));
                return r.getRetrievalStatistics("").get(key);
            }
        } catch (IOException e) {
            return super.get(key);
        }
        return super.get(key);
    }

}
