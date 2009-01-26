// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.scoring;

/**
 *
 * @author trevor
 */
public class LinearWeighting {
    double lambda;
    double background;

    /** Creates a new instance of LinearWeighting */
    public LinearWeighting(double lambda, double background) {
        this.lambda = lambda;
        this.background = background;
    }

    public double score(int count, int length) {
        double foreground = (double) count / (double) length;
        return (1 - lambda) * foreground + lambda * background;
    }
}
