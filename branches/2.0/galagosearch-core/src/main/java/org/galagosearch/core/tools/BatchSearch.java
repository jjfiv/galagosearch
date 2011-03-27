// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.tools;

import java.io.PrintStream;
import java.util.List;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.SimpleQuery;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.retrieval.RetrievalFactory;
import org.galagosearch.core.util.CallTable;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

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
    List<Parameters.Value> queries = parameters.list("query");

    // open index
    Retrieval retrieval = RetrievalFactory.instance(parameters);

    // record results requested
    int requested = (int) parameters.get("count", 1000);

    // for each query, run it, get the results, print in TREC format
    int index = 0;
    for (Parameters.Value query : queries) {

      String queryText = query.get("text");
      Parameters p = new Parameters();
      p.add("requested", Integer.toString(requested));
      String retrievalGroup = query.get("retrievalGroup", "all");
      p.add("retrievalGroup", retrievalGroup);
      Node root = StructuredQuery.parse(queryText);
      Node transformed = retrieval.transformRankedQuery(root, retrievalGroup);
      ScoredDocument[] results = retrieval.runRankedQuery(transformed, p);
      for (int i = 0; i < results.length; i++) {
        double score = results[i].score;
        int rank = i + 1;

        out.format("%s Q0 %s %d %s galago\n", query.get("number"), results[i].documentName, rank,
                formatScore(score));
      }
      index++;
      if (parameters.get("print_calls", "false").equals("true")) {
        CallTable.print(System.err, Integer.toString(index));
      }
      CallTable.reset();
    }

  }

  public static void xCount(String[] args, PrintStream out) throws Exception {
    Parameters p = new Parameters(Utility.subarray(args, 1));
    Retrieval r = RetrievalFactory.instance(p);

    String defPart = "";
    Parameters availableParts = r.getAvailableParts("all");
    List<String> available = availableParts.stringList("part");
    if (available.contains("stemmedPostings")) {
      defPart = "stemmedPostings";
    } else if (available.contains("postings")) {
      defPart = "postings";
    }

    long count;
    for (Parameters.Value v : p.list("x")) {
      String q = v.toString();
      System.err.println(q);
      if (q.contains("#")) {
        count = r.xCount(q);
      } else {
        String termOp = "#counts:" + q + ":part=" + defPart + "()";
        count = r.xCount(termOp);
      }
      out.println(count + "\t" + q);
    }
    r.close();
  }

  public static void docCount(String[] args, PrintStream out) throws Exception {
    Parameters p = new Parameters(Utility.subarray(args, 1));
    Retrieval r = RetrievalFactory.instance(p);

    String defPart = "";
    Parameters availableParts = r.getAvailableParts("all");
    List<String> available = availableParts.stringList("part");
    if (available.contains("stemmedPostings")) {
      defPart = "stemmedPostings";
    } else if (available.contains("postings")) {
      defPart = "postings";
    }

    long count;
    for (Parameters.Value v : p.list("x")) {
      String q = v.toString();
      if (q.contains("#")) {
        count = r.docCount(q);
      } else {
        String termOp = "#counts:" + q + ":part=" + defPart + "()";
        count = r.docCount(termOp);
      }
      out.println(count + "\t" + q);
    }
    r.close();
  }

  public static void main(String[] args) throws Exception {
    run(args, System.out);
  }
}
