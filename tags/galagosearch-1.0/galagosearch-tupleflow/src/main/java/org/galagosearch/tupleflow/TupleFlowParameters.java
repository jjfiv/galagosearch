// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow;

import java.io.IOException;

/**
 *
 * @author trevor
 */
public interface TupleFlowParameters {
    public Parameters getXML();
    public TypeReader getTypeReader(String specification) throws IOException;
    public Processor getTypeWriter(String specification) throws IOException;
    public Counter getCounter(String name);

    public boolean readerExists(String specification, String className, String[] order);
    public boolean writerExists(String specification, String className, String[] order);
}
