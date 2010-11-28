// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.scoring;

import java.io.IOException;
import org.galagosearch.core.retrieval.structured.CountIterator;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.core.retrieval.structured.ScoringFunctionIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength"})
public class JelinekMercerScorer extends ScoringFunctionIterator {
    double background;
    double lambda;

    public JelinekMercerScorer(Parameters parameters, CountIterator iterator) throws IOException {
        super(iterator);

        lambda = parameters.get("lambda", 0.5);
        if (parameters.containsKey("collectionProbability")) {
            background = parameters.get("collectionProbability", 0.0001);
        } else {
            long collectionLength = parameters.get("collectionLength",  0L);
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
        double foreground = (double)count/(double)length;
        return Math.log((lambda * foreground) + ((1-lambda) * background));
    }

}
