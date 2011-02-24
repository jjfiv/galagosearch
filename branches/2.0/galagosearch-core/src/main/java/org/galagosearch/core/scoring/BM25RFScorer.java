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
 * So, this scoring function is quite nice because it gives a flat score
 * to all requests. It is the responsiblity of the iterator *using* this function
 * to know when to call it (i.e. only for documents in its iteration list), otherwise
 * every identifier in the collection will get a flat boost, changing nothing. The intention
 * is that only the documents in the target term's posting list will get this score
 * increment.
 *
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"documentCount"})
public class BM25RFScorer implements ScoringFunction {

    double value;

    public BM25RFScorer(Parameters parameters, CountIterator iterator) throws IOException {
        int rt = (int) parameters.get("rt", 0);
        int R = (int) parameters.get("R", 0);
        int N = (int) parameters.get("documentCount", 0);
        double factor = parameters.get("factor", 0.33D);
        // now get idf
        long ft = 0;
        if (parameters.containsKey("ft")) {
            ft = (int) parameters.get("ft", 0);
        } else {
          ft = iterator.totalEntries();
        }
        assert(ft >= rt); // otherwise they're wrong and/or lying
        double numerator = (rt + 0.5) / (R - rt + 0.5);
        double denominator = (ft - rt + 0.5) / (N - ft - R + rt + 0.5);
        value = factor * Math.log(numerator / denominator);
    }

    public double score(int count, int length) {
        return value;
    }

    public String getParameterString(){
      return "bm25rf.value=" + value;
    }

}
