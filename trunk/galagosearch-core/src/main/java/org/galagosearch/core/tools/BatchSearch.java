// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.SimpleQuery;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor, irmarc
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
    List<Parameters.Value> queries = parameters.list("query");

    // open index
    Retrieval retrieval = Retrieval.instance(parameters.get("index"), parameters);

    // record results requested
    int requested = (int) parameters.get("count", 1000);

    // for each query, run it, get the results, look up the docnos, print in TREC format
    for (Parameters.Value query : queries) {
      // parse the query
      String queryText = query.get("text");
      Node queryRoot = parseQuery(queryText, parameters);
      queryRoot = retrieval.transformQuery(queryRoot);

      ScoredDocument[] results = retrieval.runQuery(queryRoot, requested);

      for (int i = 0; i < results.length; i++) {
        String document = retrieval.getDocumentName(results[i].document);
        double score = results[i].score;
        int rank = i + 1;

        out.format("%s Q0 %s %d %s galago\n", query.get("number"), document, rank,
                formatScore(score));
      }
    }
  }

  private static String _getDocumentName(ArrayList<Retrieval> retrievals, ScoredDocument document) throws IOException {
    int remoteID = _mapToRemote(document, retrievals.size());
    return retrievals.get(document.source).getDocumentName(remoteID);
  }

  // This is being done in serial for now to make sure it can even do that.
  private static ScoredDocument[] _runQuery(ArrayList<Retrieval> retrievals, String queryText, Parameters parameters,
          int resultsRequested) throws Exception {
    List<ScoredDocument> scored = new ArrayList<ScoredDocument>();

    // Prepare the query
    Node root = null;

    // Asynchronous retrieval
    for (int i = 0; i < retrievals.size(); i++) {
      Retrieval r = retrievals.get(i);
      if (r.isLocal()) {
        if (root == null) { // "on-demand"
          root = parseQuery(queryText, parameters);
          root = r.transformQuery(root);
        }
        r.runAsynchronousQuery(root, resultsRequested, scored, i);
      } else {
        r.runAsynchronousQuery(queryText, resultsRequested, scored, i);
      }
    }

    // Let them do their thing
    for (Retrieval r : retrievals) {
      r.join();
    }

    // Final sort / trim
    Collections.sort(scored);
    Collections.reverse(scored);
    int trimTo = Math.max(Math.min(resultsRequested, scored.size()), 0);
    scored = scored.subList(0, trimTo);
    _mapToLocal(scored, retrievals.size());
    return scored.toArray(new ScoredDocument[0]);
  }

  /**
   * Taken from the Indri id mapping function, we remap the remote document ids to
   * ensure we have unique ids here. Function is:
   * numRetrievals = retrievals.size();
   * localID = remoteID * numRetrievals + index;
   * So, for document 6 from retrieval 3 (out of 7), the localized docID would be:
   * (6 * 7) + 3 = 45.
   *
   * This function has the nice property that if there is only one server running,
   * localID == remoteID.
   */
  public static void _mapToLocal(List<ScoredDocument> docs, int numRetrievals) {
    for (ScoredDocument sd : docs) {
      sd.document = (sd.document * numRetrievals) + sd.source;
    }
  }

  public static int _mapToRemote(ScoredDocument sd, int numRetrievals) {
    return ((sd.document - sd.source) / numRetrievals);
  }

  public static void main(String[] args) throws Exception {
    run(args, System.out);
  }
}
