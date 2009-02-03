/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.parse;

import java.io.IOException;
import java.util.ArrayList;
import junit.framework.TestCase;
import org.galagosearch.core.types.WordCount;
import org.galagosearch.tupleflow.FakeParameters;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Step;

/**
 *
 * @author trevor
 */
public class WordCounterTest extends TestCase {
    
    public WordCounterTest(String testName) {
        super(testName);
    }

    private static class PostStep implements WordCount.Processor {
        public ArrayList<WordCount> results = new ArrayList<WordCount>();
        public void process(WordCount o) {
            results.add((WordCount)o);
        }
        public void close() {}
    };

    public void testCountUnigrams() throws IOException, IncompatibleProcessorException {
        Parameters p = new Parameters();
        p.add("width", "1");
        WordCounter counter = new WordCounter(new FakeParameters(p));
        Document document = new Document();
        PostStep post = new PostStep();

        counter.setProcessor(post);

        document.terms = new ArrayList<String>();
        document.terms.add("one");
        document.terms.add("two");
        document.terms.add("one");
        counter.process(document);

        assertEquals(2, post.results.size());

        for (int i = 0; i < post.results.size(); ++i) {
            WordCount wc = post.results.get(i);
            if (wc.word.equals("one")) {
                assertEquals(2, wc.count);
            } else {
                assertEquals(1, wc.count);
            }
        }
    }
}
