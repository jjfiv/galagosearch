// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.tupleflow;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 * Writes a stream of tuples to a text file.  Useful for debugging or as
 * output for simple jobs.
 *
 * @author trevor
 */

public class TextWriter<T extends Type> implements Processor<T> {
    BufferedWriter writer;

    public TextWriter(TupleFlowParameters parameters) throws IOException {
        writer = new BufferedWriter(new FileWriter(parameters.getXML().get("filename")));
    }

    public void process(T object) throws IOException {
        writer.write(object.toString());
        writer.write("\n");
    }

    public void close() throws IOException {
        writer.close();
    }

    public static String getInputClass(TupleFlowParameters parameters) {
        return parameters.getXML().get("class");
    }

    public static boolean verify(TupleFlowParameters parameters, ErrorHandler handler) {
        Parameters p = parameters.getXML();
        if (!Verification.requireParameters(new String[] { "filename", "class" }, p, handler))
            return false;
        if (!Verification.requireClass(p.get("class"), handler))
            return false;
        if (!Verification.requireWriteableFile(p.get("filename"), handler))
            return false;
        return true;
    }
}
