/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.parse;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.ExNihiloSource;
import org.galagosearch.tupleflow.FileSource;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.ErrorHandler;

/**
 *
 * @author marc
 */
@OutputClass(className="org.galagosearch.core.types.KeyValuePair", order={"+key"})
public class VocabularySource implements ExNihiloSource<KeyValuePair> {
    Counter vocabCounter;

    public Processor<KeyValuePair> processor;
    TupleFlowParameters parameters;
    IndexReader.Iterator iterator;

    public VocabularySource(TupleFlowParameters parameters) throws Exception {
        String indexPath = parameters.getXML().get("directory");
        String partPath = StructuredIndex.getPartPath(indexPath, parameters.getXML().get("part"));
        IndexReader reader = new IndexReader(partPath);
        iterator = reader.getIterator();
    }

    public void run() throws IOException {
        KeyValuePair kvp;
        while (!iterator.isDone()) {
            kvp = new KeyValuePair();
            kvp.key = iterator.getKey();
            kvp.value = new byte[0];
            processor.process(kvp);
            iterator.nextKey();
        }
        processor.close();
    }

    public void setProcessor(Step processor) throws IncompatibleProcessorException {
        Linkage.link(this, processor);
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        FileSource.verify(parameters, handler);
        if (!parameters.getXML().containsKey("part")) {
            handler.addError("Need a part to read from.");
        }

        String partPath = StructuredIndex.getPartPath(parameters.getXML().get("directory"), parameters.getXML().get("part"));
        try {
            if (!IndexReader.isIndexFile(partPath)) {
                handler.addError(partPath + " is not an index file.");
            }
        } catch (FileNotFoundException fnfe) {
            handler.addError(partPath + " could not be found.");
        } catch (IOException ioe) {
            handler.addError("Generic IO error: " + ioe.getMessage());
        }
    }
}
