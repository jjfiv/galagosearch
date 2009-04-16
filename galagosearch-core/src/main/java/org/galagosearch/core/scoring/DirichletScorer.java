// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.scoring;

import java.io.IOException;
import org.galagosearch.core.retrieval.structured.CountIterator;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.core.retrieval.structured.ScoringFunctionIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
@RequiredStatistics(statistics = {"collectionLength"})
public class DirichletScorer extends ScoringFunctionIterator {
    double background;
    double mu;

    public DirichletScorer(Parameters parameters, CountIterator iterator) throws IOException {
        super(iterator);

        mu = parameters.get("mu", 1500);
        if (parameters.containsKey("collectionProbability")) {
            background = parameters.get("collectionProbability", 0.0001);
        } else {
            long collectionLength = parameters.get("collectionLength", (long)0);
            long count = 0;
            
            while (!iterator.isDone()) {
                count += iterator.count();
                iterator.nextDocument();
            }

            if (count > 0) {
                background = (double)count / (double)collectionLength;
            } else {
                background = 0.5 / (double)collectionLength;
            }
            iterator.reset();
        }
    }

    public double scoreCount(int count, int length) {
        double numerator = count + mu * background;
        double denominator = length + mu;

        return Math.log(numerator / denominator);
    }
}

