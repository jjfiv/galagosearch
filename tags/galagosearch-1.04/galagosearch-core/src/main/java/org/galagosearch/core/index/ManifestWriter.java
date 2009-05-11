// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index;

import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;
import org.galagosearch.tupleflow.types.XMLFragment;
import java.io.File;
import java.io.IOException;

/**
 *    
 * @author trevor
 */
@InputClass(className = "org.galagosearch.tupleflow.types.XMLFragment")
public class ManifestWriter implements Processor<XMLFragment> {
    String filename;
    Parameters result;

    /** Creates a new instance of ManifestWriter */
    public ManifestWriter(TupleFlowParameters p) throws IOException {
        if (p.getXML().containsKey("xml")) {
            Parameters.Value value = p.getXML().value().map().get("xml").get(0);
            result = new Parameters(value);
        } else {
            result = new Parameters();
        }

        filename = p.getXML().get("filename");
    }

    public void process(XMLFragment object) throws IOException {
        result.add(object.nodePath, object.innerText);
    }

    public void close() throws IOException {
        File f = new File(filename);
        String parent = f.getParent();

        // make parent directories
        if (parent != null) {
            new File(parent).mkdirs();
        }
        result.write(filename);
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        if (!parameters.getXML().containsKey("filename")) {
            handler.addError("ManifestWriter requires a 'filename' parameter.");
            return;
        }

        File f = new File(parameters.getXML().get("filename"));

        if (f.isFile() && f.canWrite()) {
            return; // good news
        }
        if (f.isDirectory()) {
            handler.addError("Pathname " + f.toString() + " exists, and it is a directory, but " +
                    "ManifestWriter would like to write a file there.");
            return;
        }

        // this will search upwards and verify that we can make
        // the necessary directory structure to store this file.
        Verification.requireWriteableFile(f.toString(), handler);
    }
}
