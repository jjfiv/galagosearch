// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.scoring;

public interface WeightedTerm extends Comparable<WeightedTerm> {
    public String getTerm();
    public double getWeight();
    public int compareTo(WeightedTerm other);
}