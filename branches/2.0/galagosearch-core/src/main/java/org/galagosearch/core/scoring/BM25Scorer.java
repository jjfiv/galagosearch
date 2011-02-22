/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.scoring;

import java.io.IOException;
import org.galagosearch.core.index.PositionIndexReader;
import org.galagosearch.core.retrieval.structured.CountIterator;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount"})
public class BM25Scorer implements ScoringFunction {

    double b;
    double k;
    double avgDocLength;
    double idf;

    public BM25Scorer(Parameters parameters, CountIterator iterator) throws IOException {
        b = parameters.get("b", 0.75D);
        k = parameters.get("k", 1.2D);

        long collectionLength = parameters.get("collectionLength", 0L);
        long documentCount = parameters.get("documentCount", 0L);
        avgDocLength = (collectionLength + 0.0) / (documentCount + 0.0);

        // now get idf
        long df = 0;
        if (parameters.containsKey("df")) {
            df = parameters.get("df", 0L);
        } else {
            if (iterator instanceof PositionIndexReader.Iterator) {
                df = ((PositionIndexReader.Iterator) iterator).totalDocuments();
            } else {
                while (!iterator.isDone()) {
                    df++;
                    iterator.next();
                }
                iterator.reset();
            }
        }
        idf = Math.log((documentCount - df + 0.5) / (df + 0.5));
    }

    public double score(int count, int length) {
        double numerator = count * (k + 1);
        double denominator = count + (k * (1 - b + (b * length / avgDocLength)));
        return idf * numerator / denominator;
    }

    public String getParameterString(){
      return "bm25.b=" + b + ",bm25.k="+k;
    }
}
