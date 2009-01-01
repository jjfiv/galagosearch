// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.scoring;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class DistributionSmootherFactory {
    public static DistributionSmoother newInstance(Map<String, String> feature, HashMap<String, Double> backgrounds) {
        String smootherName = "dirichlet";

        if (feature.containsKey("smoothing")) {
            smootherName = feature.get("smoothing");
        }

        DistributionSmoother smoother;

        if (smootherName.equals("linear") || smootherName.equals("jm")) {
            double lambda = 0.4;
            if (feature.containsKey("lambda")) {
                lambda = Double.parseDouble(feature.get("lambda"));
            }
            smoother = new LinearSmoother(lambda, backgrounds);
        } else {
            double mu = 2500;
            if (feature.containsKey("mu")) {
                mu = Double.parseDouble(feature.get("mu"));
            }
            smoother = new DirichletSmoother(mu, backgrounds);
        }

        return smoother;
    }

    public static DistributionSmoother newInstance(Parameters.Value feature, HashMap<String, Double> backgrounds) {
        Set<String> keys = new HashSet<String>();
        if (feature != null) {
            keys = feature.map().keySet();
        }
        HashMap<String, String> map = new HashMap();

        for (String key : keys) {
            map.put(key, feature.get(key));
        }

        return newInstance(map, backgrounds);
    }

    public static DistributionSmoother newInstance(Parameters.Value feature) {
        return newInstance(feature, new HashMap());
    }
}
