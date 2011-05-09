// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.parse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.galagosearch.core.types.DocumentProbability;
import org.galagosearch.tupleflow.ExNihiloSource;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 *
 * @author trevor
 */
@OutputClass(className = "org.galagosearch.core.types.DocumentProbability")
public class PriorParser implements ExNihiloSource<DocumentProbability> {
    public Processor<DocumentProbability> processor;
    BufferedReader reader;

    public PriorParser(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
        String fileName = parameters.getXML().get("filename");
        reader = new BufferedReader(new FileReader(fileName));
    }

    public void setProcessor(Step processor) throws IncompatibleProcessorException {
        Linkage.link(this, processor);
    }

    public void run() throws IOException {
        String line;

        while ((line = reader.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            String[] fields = line.split(" ");
            if (fields.length != 2) {
                continue;
            }
            String document = fields[0];
            String probability = fields[1];

            processor.process(new DocumentProbability(document, Double.parseDouble(probability)));
        }

        processor.close();
    }

    public Class<DocumentProbability> getOutputClass() {
        return DocumentProbability.class;
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        if (!Verification.requireParameters(new String[]{"filename"}, parameters.getXML(), handler)) {
            return;
        }
        String filename = parameters.getXML().get("filename");
        if (!new File(filename).isFile()) {
            handler.addError("File " + filename + " does not exist.");
        }
    }
}
