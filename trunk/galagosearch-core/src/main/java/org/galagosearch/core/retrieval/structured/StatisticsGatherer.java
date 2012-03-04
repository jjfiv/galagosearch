// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class StatisticsGatherer {
    CountIterator iterator;
    long documentCount;
    long termCount;
    long collectionLength;
    double averageDocumentLength;
    HashSet<String> required;

    public StatisticsGatherer(CountIterator iterator) {
        this.iterator = iterator;
        this.documentCount = 0;
        this.termCount = 0;
        this.collectionLength = 0;
        this.averageDocumentLength = 0;
        this.required = new HashSet<String>();
    }

    public StatisticsGatherer(StructuredIndex index, CountIterator iterator, String[] required) {
        this(iterator);

        this.required.addAll(Arrays.asList(required));
        collectionLength = index.getCollectionLength();
        averageDocumentLength = (double) index.getCollectionLength() / (double) index.
                getDocumentCount();
    }

    public void run() throws IOException {
        while (!iterator.isDone()) {
            documentCount += 1;
            termCount += iterator.count();

            iterator.nextDocument();
        }
    }

    public long getDocumentCount() {
        return documentCount;
    }

    public long getTermCount() {
        return termCount;
    }

    public double getAverageDocumentLength() {
        return averageDocumentLength;
    }

    public long getCollectionLength() {
        return collectionLength;
    }

    public double getCollectionProbability() {
        return Math.max((double) termCount, 0.5) / (double) collectionLength;
    }

    public void store(Parameters p) {
        if (!p.containsKey("collectionLength")) {
            p.add("collectionLength", Long.toString(
                                                      getCollectionLength()));
        }
        if (!p.containsKey("averageDocumentLength")) {
            p.add("averageDocumentLength", Double.toString(
                                                           getAverageDocumentLength()));
        }
        if (!p.containsKey("termCount")) {
            p.add("termCount", Long.toString(getTermCount()));
        }
        if (!p.containsKey("documentCount")) {
            p.add("documentCount",
                                                   Long.toString(getDocumentCount()));
        }
        if (!p.containsKey("collectionProbability")) {
            p.add("collectionProbability", Double.toString(
                                                           getCollectionProbability()));
        }
    }
}
