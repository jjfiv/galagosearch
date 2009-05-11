// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.parse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.galagosearch.core.types.WordCount;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Reducer;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author trevor
 */
@Verified
@InputClass(className = "org.galagosearch.core.parse.Document")
@OutputClass(className = "org.galagosearch.core.types.WordCount")
public class WordCounter extends StandardStep<Document, WordCount> implements Reducer<WordCount> {
    int maxWidth = 1;
    HashSet<String> filterWords;

    public WordCounter(TupleFlowParameters parameters) throws IOException {
        maxWidth = (int) parameters.getXML().get("width", 1);
        String filename = parameters.getXML().get("filter", (String) null);
        if (filename != null) {
            filterWords = Utility.readFileToStringSet(new File(filename));
        } else {
            filterWords = null;
        }
    }

    public void process(Document document) throws IOException {
        List<String> tokens = document.terms;
        HashMap<String, WordCount> countObjects = new HashMap<String, WordCount>();

        for (int i = 0; i < tokens.size(); i++) {
            String token = tokens.get(i);

            if (token == null) {
                continue;
            }
            updateCounts(token, countObjects);

            if (maxWidth > 1) {
                StringBuilder builder = new StringBuilder();
                builder.append(token);

                int end = Math.min(i + maxWidth, tokens.size());
                for (int j = i + 1; j < end; j++) {
                    token = tokens.get(j);

                    if (token == null) {
                        break;
                    }
                    builder.append(' ');
                    builder.append(token);

                    updateCounts(builder.toString(), countObjects);
                }
            }
        }

        for (WordCount count : countObjects.values()) {
            assert count != null;
            assert count.word != null;
            processor.process(count);
        }
    }

    public ArrayList<WordCount> reduce(List<WordCount> input) throws IOException {
        HashMap<String, WordCount> countObjects = new HashMap<String, WordCount>(input.size() / 5);

        for (WordCount wordCount : input) {
            WordCount original = countObjects.get(wordCount.word);

            if (original == null) {
                countObjects.put(wordCount.word, original);
            } else {
                original.documents += wordCount.documents;
                original.count += wordCount.count;
            }
        }

        return new ArrayList<WordCount>(countObjects.values());
    }

    void updateCounts(String token, HashMap<String, WordCount> countObjects) {
        WordCount wordCount = countObjects.get(token);

        if (filterWords != null && !filterWords.contains(token)) {
            return;
        }
        if (wordCount != null) {
            wordCount.count += 1;
        } else {
            wordCount = new WordCount(new String(token), 1, 1);
            countObjects.put(token, wordCount);
        }
    }
}
