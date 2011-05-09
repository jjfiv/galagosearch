/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.parse;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.HashSet;
import org.galagosearch.core.index.GenericIndexReader;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.ExNihiloSource;
import org.galagosearch.tupleflow.FileSource;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;

/**
 *
 * @author marc
 */
@OutputClass(className="org.galagosearch.core.types.KeyValuePair", order={"+key"})
public class VocabularySource implements ExNihiloSource<KeyValuePair> {
    Counter vocabCounter;
    Counter skipCounter;

    public Processor<KeyValuePair> processor;
    TupleFlowParameters parameters;
    GenericIndexReader reader;
    GenericIndexReader.Iterator iterator;
    HashSet<String> inclusions = null;    
    HashSet<String> exclusions = null;

    public VocabularySource(TupleFlowParameters parameters) throws Exception {
        String partPath = parameters.getXML().get("filename");
        reader = GenericIndexReader.getIndexReader(partPath);
	vocabCounter = parameters.getCounter("terms read");
	skipCounter = parameters.getCounter("terms skipped");
	iterator = reader.getIterator();
	
	// Look for queries to base the extraction
        Parameters p = parameters.getXML();
	if (p.containsKey("include")) {
	    List<String> inc = p.stringList("include");
	    inclusions = new HashSet<String>();
	    for (String s : inc) {
		inclusions.add(s);
	    }
	}

	if (p.containsKey("exclude")) {
	    List<String> inc = p.stringList("exclude");
	    exclusions = new HashSet<String>();
	    for (String s : inc) {
		exclusions.add(s);
	    }
	}	
    }

    public void run() throws IOException {
        KeyValuePair kvp;
	while (!iterator.isDone()) {
	    
	    // Filter if we need to
	    if (inclusions != null || exclusions != null) {
		String s = Utility.toString(iterator.getKey());
		if (inclusions != null && inclusions.contains(s) == false) {
		    iterator.nextKey();
		    if (skipCounter != null) skipCounter.increment();
		    continue;
		}
	    
		if (exclusions != null && exclusions.contains(s) == true) {
		    iterator.nextKey();
		    if (skipCounter != null) skipCounter.increment();
		    continue;
		}
	    }

            kvp = new KeyValuePair();
            kvp.key = iterator.getKey();
            kvp.value = new byte[0];
            processor.process(kvp);
            if (vocabCounter != null) vocabCounter.increment();
    	    iterator.nextKey();
        }
        processor.close();
	reader.close();
    }

    public void setProcessor(Step processor) throws IncompatibleProcessorException {
        Linkage.link(this, processor);
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        FileSource.verify(parameters, handler);
        String partPath = parameters.getXML().get("filename");
        try {
            if (!GenericIndexReader.isIndex(partPath)){
              handler.addError(partPath + " is not an index file.");
            }
        } catch (FileNotFoundException fnfe) {
            handler.addError(partPath + " could not be found.");
        } catch (IOException ioe) {
            handler.addError("Generic IO error: " + ioe.getMessage());
        }
    }
}
