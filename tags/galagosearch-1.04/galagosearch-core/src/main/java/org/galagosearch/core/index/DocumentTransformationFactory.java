// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.util.ArrayList;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.parse.Porter2Stemmer;
import org.galagosearch.core.parse.WordFilter;
import java.util.HashSet;

/**
 *
 * @author trevor
 */
public class DocumentTransformationFactory {
    public static Processor<Document> instance(Parameters parameters, String key) {
        Processor<Document> processor = null;

        if (key.equals("stopper") && parameters.containsKey("stopper")) {
            HashSet<String> stopwords = new HashSet<String>(parameters.stringList("stopper/word"));
            processor = new WordFilter(stopwords);
        }

        if (key.equals("stemmer") && parameters.get("stemmer", "none").equals("porter2")) {
            processor = new Porter2Stemmer();
        }

        return processor;
    }

    public static ArrayList<Processor<Document>> instance(Parameters parameters) {
        ArrayList<Processor<Document>> transformations = new ArrayList<Processor<Document>>();
        String[] transformationNames = {"stopper", "stemmer"};

        for (String name : transformationNames) {
            Processor<Document> transformation = instance(parameters, name);

            if (transformation != null) {
                transformations.add(transformation);
            }
        }

        return transformations;
    }
}
