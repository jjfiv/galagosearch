// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.scoring;

import java.io.IOException;
import org.galagosearch.core.index.PositionIndexReader;
import org.galagosearch.core.retrieval.structured.CountValueIterator;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength","lambda"})
public class JelinekMercerScorer implements ScoringFunction {

    double background;
    double lambda;

    public JelinekMercerScorer(Parameters parameters, CountValueIterator iterator) throws IOException {

        lambda = parameters.get("lambda", 0.5);
        if (parameters.containsKey("collectionProbability")) {
            background = parameters.get("collectionProbability", 0.0001);
        } else {
            long collectionLength = parameters.get("collectionLength", 0L);
            long count = 0;

            if (PositionIndexReader.AggregateIterator.class.isInstance(iterator)) {
                count = ((PositionIndexReader.AggregateIterator) iterator).totalPositions();
            } else {
                while (!iterator.isDone()) {
                    count += iterator.count();
                    iterator.next();
                }
            }

            if (count > 0) {
                background = (double) count / (double) collectionLength;
            } else {
                background = 0.5 / (double) collectionLength;
            }
            iterator.reset();
        }
    }

    public double score(int count, int length) {
        double foreground = (double) count / (double) length;
        return Math.log((lambda * foreground) + ((1-lambda) * background));
    }

    public String getParameterString(){
      return "jm.lambda=" + lambda;
    }
}

