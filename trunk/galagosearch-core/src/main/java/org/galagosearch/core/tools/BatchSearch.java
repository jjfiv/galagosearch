// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.SimpleQuery;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class BatchSearch {
    public static Node parseQuery(String query, Parameters parameters) {
        String queryType = parameters.get("queryType", "complex");

        if (queryType.equals("simple")) {
            return SimpleQuery.parseTree(query);
        }

        return StructuredQuery.parse(query);
    }

    public static String formatScore(double score) {
        double difference = Math.abs(score - (int) score);

        if (difference < 0.00001) {
            return Integer.toString((int) score);
        }
        return String.format("%10.8f", score);
    }

    public static void run(String[] args, PrintStream out) throws Exception {
        // read in parameters
        Parameters parameters = new Parameters(args);
	System.out.println("params: " + parameters.toString());
        List<Parameters.Value> queries = parameters.list("query");

        // open index
        Retrieval retrieval = Retrieval.instance(parameters.get("index"), parameters);
	
        // record results requested
        int requested = (int) parameters.get("count", 1000);

        // for each query, run it, get the results, print in TREC format
        for (Parameters.Value query : queries) {

            String queryText = query.get("text");

            Parameters p = new Parameters();
            p.add("requested", Integer.toString(requested));
            Node root = StructuredQuery.parse(queryText);
            Node transformed = retrieval.transformQuery(root, "all");
            ScoredDocument[] results = retrieval.runQuery(transformed, p);
            for (int i = 0; i < results.length; i++) {
                //String document = retrieval.getDocumentName(results[i].document);
                double score = results[i].score;
                int rank = i + 1;

                out.format("%s Q0 %s %d %s galago\n", query.get("number"), results[i].documentName, rank,
                           formatScore(score));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        run(args, System.out);
    }
}
