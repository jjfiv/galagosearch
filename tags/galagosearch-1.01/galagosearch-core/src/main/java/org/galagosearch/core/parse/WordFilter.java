// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.parse;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.NullProcessor;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Source;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;

/**
 * WordFilter filters out unnecessary words from documents.  Typically this object
 * takes a stopword list as parameters and removes all the listed words.  However, 
 * this can also be used to keep only the specified list of words in the index, which
 * can be used to create an index that is tailored for only a small set
 * of experimental queries.
 * 
 * @author trevor
 */
@InputClass(className = "org.galagosearch.core.parse.Document")
@OutputClass(className = "org.galagosearch.core.parse.Document")
public class WordFilter implements Processor<Document>, Source<Document> {
    Set<String> stopwords = new HashSet<String>();
    boolean keepListWords = false;
    public Processor<Document> processor = new NullProcessor(Document.class);

    public WordFilter(HashSet<String> words) {
        stopwords = words;
    }

    public WordFilter(TupleFlowParameters params) throws IOException {
        if (params.getXML().containsKey("filename")) {
            String filename = params.getXML().get("filename");
            stopwords = Utility.readFileToStringSet(new File(filename));
        } else {
            stopwords = new HashSet(params.getXML().stringList("word"));
        }

        keepListWords = params.getXML().get("keepListWords", false);
    }

    public void process(Document document) throws IOException {
        List<String> words = document.terms;

        for (int i = 0; i < words.size(); i++) {
            String word = words.get(i);
            boolean wordInList = stopwords.contains(word);
            boolean removeWord = wordInList != keepListWords;

            if (removeWord) {
                words.set(i, null);
            }
        }

        processor.process(document);
    }

    public void close() throws IOException {
        processor.close();
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        if (parameters.getXML().containsKey("filename")) {
            return;
        }
        if (parameters.getXML().stringList("word").size() == 0) {
            handler.addWarning("Couldn't find any words in the stopword list.");
        }
    }

    public void setProcessor(Step processor) throws IncompatibleProcessorException {
        Linkage.link(this, processor);
    }
}
