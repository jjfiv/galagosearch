// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.parse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.galagosearch.core.types.WordCount;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Reducer;
import org.galagosearch.tupleflow.Source;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author trevor
 */
@InputClass(className = "org.galagosearch.core.types.WordCount", order = {"+word"})
@OutputClass(className = "org.galagosearch.core.types.WordCount", order = {"+word"})
@Verified
public class WordCountReducer implements Processor<WordCount>, Source<WordCount>, Reducer<WordCount>,
        WordCount.Processor {
    public Processor<WordCount> processor;
    private WordCount last = null;
    private WordCount aggregate = null;
    private long totalTerms = 0;

    public void process(WordCount wordCount) throws IOException {
        if (last != null) {
            if (!wordCount.word.equals(last.word)) {
                flush();
            } else if (aggregate == null) {
                aggregate = new WordCount(last.word, last.count + wordCount.count,
                                          last.documents + wordCount.documents);
            } else {
                aggregate.count += wordCount.count;
                aggregate.documents += wordCount.documents;
            }
        }

        last = wordCount;
    }

    public void flush() throws IOException {
        if (last != null) {
            if (aggregate != null) {
                assert aggregate != null;
                processor.process(aggregate);
                totalTerms += aggregate.count;
            } else {
                assert last != null;
                processor.process(last);
                totalTerms += last.count;
            }

            aggregate = null;
        }
    }

    public void close() throws IOException {
        flush();
        processor.close();
    }

    public void setProcessor(Step processor) throws IncompatibleProcessorException {
        Linkage.link(this, processor);
    }

    public ArrayList<WordCount> reduce(List<WordCount> input) throws IOException {
        HashMap<String, WordCount> countObjects = new HashMap<String, WordCount>();

        for (WordCount wordCount : input) {
            WordCount original = countObjects.get(wordCount.word);

            if (original == null) {
                countObjects.put(wordCount.word, wordCount);
            } else {
                original.documents += wordCount.documents;
                original.count += wordCount.count;
            }
        }

        return new ArrayList<WordCount>(countObjects.values());
    }

    public long getTotalTerms() {
        return totalTerms;
    }

    public Class<WordCount> getInputClass() {
        return WordCount.class;
    }

    public Class<WordCount> getOutputClass() {
        return WordCount.class;
    }
}
