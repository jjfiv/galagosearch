// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.scoring;

import java.io.IOException;
import java.util.HashMap;
import org.galagosearch.core.types.DocumentLengthWordCount;
import org.galagosearch.core.types.DocumentWordProbability;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class LinearSmoother extends StandardStep<DocumentLengthWordCount, DocumentWordProbability>
        implements DistributionSmoother {
    double lambda;
    HashMap<String, Double> backgrounds;

    public LinearSmoother(Parameters.Value value, HashMap<String, Double> backgrounds) {
        double lm = 0.4;

        if (value.containsKey("lambda")) {
            lm = Double.parseDouble(value.get("lambda"));
        }

        this.lambda = lm;
        this.backgrounds = backgrounds;
    }

    public LinearSmoother(double lambda, HashMap<String, Double> backgrounds) {
        this.lambda = lambda;
        this.backgrounds = backgrounds;
    }

    public void process(DocumentLengthWordCount object) throws IOException {
        double background = backgrounds.get(object.word);
        double foreground = 0;

        if (object.length > 0) {
            foreground = (double) object.count / (double) object.length;
        }
        double probability = lambda * foreground + (1 - lambda) * background;
        processor.process(new DocumentWordProbability(object.document,
                                                      Utility.makeBytes(object.word), probability));
    }

    public double smooth(double background, int count, int length) {
        return (1 - lambda) * (double) count / (double) length + lambda * background;
    }

    public double smooth(String word, int count, int length) {
        return smooth(backgrounds.get(word), count, length);
    }

    public Class<DocumentLengthWordCount> getInputClass() {
        return DocumentLengthWordCount.class;
    }

    public Class<DocumentWordProbability> getOutputClass() {
        return DocumentWordProbability.class;
    }
}
