// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.scoring;

import java.io.IOException;
import org.galagosearch.core.index.PositionIndexReader;
import org.galagosearch.core.retrieval.structured.CountValueIterator;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
@RequiredStatistics(statistics = {"collectionLength"})
public class DirichletScorer implements ScoringFunction {

    double background;
    double mu;

    public DirichletScorer(Parameters parameters, CountValueIterator iterator) throws IOException {

        mu = parameters.get("mu", 1500);
        if (parameters.containsKey("collectionProbability")) {
            background = parameters.get("collectionProbability", 0.0001);
        } else {
            long collectionLength = parameters.get("collectionLength", (long) 0);
            long count = 0;

            if (PositionIndexReader.AggregateIterator.class.isInstance(iterator)) {
                count = ((PositionIndexReader.AggregateIterator) iterator).totalPositions();
            } else {
                while (!iterator.isDone()) {
                    count += iterator.count();
                    iterator.next();
                }
            iterator.reset();
            }
            if (count > 0) {
                background = (double) count / (double) collectionLength;
            } else {
                background = 0.5 / (double) collectionLength;
            }
        }
    }

    public double score(int count, int length) {
        double numerator = count + (mu * background);
        double denominator = length + mu;
        return Math.log(numerator / denominator);
    }

    public String getParameterString(){
      return "dir.mu=" + mu;
    }

}
