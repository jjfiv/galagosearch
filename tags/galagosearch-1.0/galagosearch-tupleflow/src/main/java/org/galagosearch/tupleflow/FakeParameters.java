// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow;

import java.io.IOException;

/**
 *
 * @author trevor
 */
public class FakeParameters implements TupleFlowParameters {
    Parameters parameters;

    public FakeParameters(Parameters p) {
        this.parameters = p;
    }

    public Parameters getXML() {
        return parameters;
    }
    
    public Counter getCounter(String name) {
        return null;
    }

    public TypeReader getTypeReader(String specification) throws IOException {
        return null;
    }

    public Processor getTypeWriter(String specification) throws IOException {
        return null;
    }

    public boolean readerExists(String specification, String className, String[] order) {
        return false;
    }

    public boolean writerExists(String specification, String className, String[] order) {
        return false;
    }
}
