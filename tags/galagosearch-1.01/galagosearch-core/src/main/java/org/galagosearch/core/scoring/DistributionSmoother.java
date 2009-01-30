// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.scoring;

/**
 *
 * @author trevor
 */
public interface DistributionSmoother {
    public double smooth(String word, int count, int length);
    public double smooth(double background, int count, int length);
}
