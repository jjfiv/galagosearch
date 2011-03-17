// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.tools;

import java.io.PrintStream;
import java.util.List;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.SimpleQuery;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.retrieval.structured.RetrievalFactory;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * Runs a parameter sweep
 *  This allows a user to input a set of parameters and compute all results in parallel
 *
 *  Parameters can be expressed as a comma separated list
 *  egs
 *     mu parameter sweep:
 *    #combine( #feature:dirichlet:mu=100,200,300( term ) ... )
 *     weight parameter sweep:
 *    #combine : 0 = 1,2,3 : 1 = 3,2,1 ( term1 term2 )
 *
 *  Note that several parameter sweeps can be completed in parallel
 *  and the cross product of all parameter sweeps will be returned:
 *    #combine : 0 = 1,2 : 1 = 2,1 ( 
 *      #combine : 0 = 0.1,0.2 : 1 = 0.9,0.8 ( term1 term2 )
 *      #combine : 0 = 0.5,0.6 : 1 = 0.5,0.4 ( term3 term4 )
 *    )
 *
 *  ###IMPORTANT###
 *  Also note - #combine nodes will not perform a parameter sweep for identically parametered smoothing children
 *  In this case internal will be processed in parallel instead of by cross producting them:
 *    #combine : 0 = 1,2 : 1 = 2,1 (
 *      #feature:dirichlet:mu=100,1000(term1)
 *      #feature:dirichlet:mu=100,1000(term2)
 *    )
 *  This does not include identical any parameter string that contains an operator (as indicated by a '#')
 *  ###IMPORTANT###
 *
 * @author sjh
 */
public class BatchParameterSweep {
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
    List<Parameters.Value> queries = parameters.list("query");

    // open index
    Retrieval retrieval = RetrievalFactory.instance(parameters);

    // record results requested
    int requested = (int) parameters.get("count", 1000);

    // for each query, run it, get the results, print in TREC format
    for (Parameters.Value query : queries) {

      String queryText = query.get("text");

      Parameters p = new Parameters();
      p.add("requested", Integer.toString(requested));
      Node root = StructuredQuery.parse(queryText);
      Node transformed = retrieval.transformRankedQuery(root, "all");

      ScoredDocument[] results = retrieval.runParameterSweep(transformed, p);
      for (int i = 0; i < results.length; i++) {
        double score = results[i].score;

        out.format("%s Q0 %s %d %s galago %s\n", query.get("number"), results[i].documentName, results[i].rank,
                formatScore(score), results[i].params);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    run(args, System.out);
  }
}
