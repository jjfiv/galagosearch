// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.scoring;

import java.io.IOException;
import java.util.HashMap;
import org.galagosearch.core.types.DocumentLengthWordCount;
import org.galagosearch.core.types.DocumentWordProbability;
import org.galagosearch.core.types.WordProbability;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.TypeReader;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 *
 * @author trevor
 */
public class DirichletSmoother extends StandardStep<DocumentLengthWordCount, DocumentWordProbability>
        implements DistributionSmoother {
    double mu;
    HashMap<String, Double> backgrounds;

    public DirichletSmoother(double mu, HashMap<String, Double> backgrounds) {
        this.mu = mu;
        this.backgrounds = backgrounds;
    }

    public DirichletSmoother(TupleFlowParameters parameters) throws IOException {
        this.mu = parameters.getXML().get("mu", 1500);
        TypeReader<WordProbability> backgroundReader = parameters.getTypeReader("background");
        WordProbability backgroundObject = null;
        this.backgrounds = new HashMap<String, Double>();

        while ((backgroundObject = backgroundReader.read()) != null) {
            backgrounds.put(backgroundObject.word, backgroundObject.probability);
        }
    }

    public void process(DocumentLengthWordCount object) throws IOException {
        double probability = smooth(object.word, object.count, object.length);
        processor.process(new DocumentWordProbability(object.document,
                                                      Utility.makeBytes(object.word), probability));
    }

    public double smooth(double background, int count, int length) {
        double numerator = count + mu * background;
        double denominator = length + mu;

        return numerator / denominator;
    }

    public double smooth(String word, int count, int length) {
        Double background = backgrounds.get(word);
        assert background != null : "Couldn't find " + word + " in backgrounds: " + backgrounds.size();
        return smooth(background, count, length);
    }

    public Class<DocumentLengthWordCount> getInputClass() {
        return DocumentLengthWordCount.class;
    }

    public Class<DocumentWordProbability> getOutputClass() {
        return DocumentWordProbability.class;
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        Verification.verifyTypeReader("background", WordProbability.class, parameters, handler);
    }
}
