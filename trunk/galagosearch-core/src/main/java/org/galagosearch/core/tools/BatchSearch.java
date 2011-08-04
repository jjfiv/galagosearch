// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.tools;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Random;
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
    requested = (int) parameters.get("requested", 1000);
    int index = 0;
    long starttime = System.currentTimeMillis();
    long sumtime = 0;

    // for each query, run it, get the results, print in TREC format
    if (parameters.containsKey("seed")) {
      long seed = parameters.get("seed", 0L);
      Random r = new Random(seed);
      Collections.shuffle(queries, r);
    }

    for (Parameters.Value query : queries) {
      String queryText = query.get("text");
      Parameters p = new Parameters();
      p.add("requested", Integer.toString(requested));
      p.add("mod", parameters.get("mod", "none"));
      String retrievalGroup = query.get("retrievalGroup", "all");
      p.add("retrievalGroup", retrievalGroup);
      Node root = StructuredQuery.parse(queryText);
      Node transformed = retrieval.transformRankedQuery(root, retrievalGroup);

      if (parameters.get("printTransformation", false)) {
        System.err.println("Text:" + queryText);
        System.err.println("Parsed Node:" + root.toString());
        System.err.println("Transformed Node:" + transformed.toString());
      }

      long querystarttime = System.currentTimeMillis();
      ScoredDocument[] results = retrieval.runRankedQuery(transformed, p);
      long queryendtime = System.currentTimeMillis();
      sumtime += queryendtime - querystarttime;

      for (int i = 0; i < results.length; i++) {
        double score = results[i].score;
        int rank = i + 1;

        out.format("%s Q0 %s %d %s galago\n", query.get("number"), results[i].documentName, rank,
		   formatScore(score));
      }
      index++;
      if (parameters.get("print_calls", "false").equals("true")) {
        CallTable.print(System.out, Integer.toString(index));
      }
      CallTable.reset();
    }

    long endtime = System.currentTimeMillis();

    if(parameters.get("time", false)){
      System.err.println("TotalTime: " + (endtime - starttime));
      System.err.println("AvgTime: " + ((endtime - starttime) / queries.size()));
      System.err.println("AvgQueryTime: " + (sumtime / queries.size()));
    }
  }

  public static void xCount(String[] args, PrintStream out) throws Exception {
    Parameters p = new Parameters(Utility.subarray(args, 1));
    Retrieval r = RetrievalFactory.instance(p);
    
    long count;
    for (Parameters.Value v : p.list("x")) {
      String q = v.toString();
      Node parsed = StructuredQuery.parse(q);
      Node transformed = r.transformCountQuery(parsed, "all");

      if(p.get("printTransformation", false)){
        System.err.println(q);
        System.err.println(parsed);
        System.err.println(transformed);
      }
      
      count = r.xCount( transformed );
      out.println(count + "\t" + q);
    }
    r.close();
  }

  public static void docCount(String[] args, PrintStream out) throws Exception {
    Parameters p = new Parameters(Utility.subarray(args, 1));
    Retrieval r = RetrievalFactory.instance(p);
    
    //String defPart = "";
    //Parameters availableParts = r.getAvailableParts("all");
    //List<String> available = availableParts.stringList("part");
    //if (available.contains("stemmedPostings")) {
    //  defPart = "stemmedPostings";
    //} else if (available.contains("postings")) {
    //  defPart = "postings";
    //}

    long count;
    for (Parameters.Value v : p.list("x")) {
      String q = v.toString();
        System.err.println(q);
      Node parsed = StructuredQuery.parse(q);
        System.err.println(parsed);
      Node transformed = r.transformCountQuery(parsed, "all");
        System.err.println(transformed);

      if(p.get("printTransformation", false)){
        System.err.println(q);
        System.err.println(parsed);
        System.err.println(transformed);
      }
      
      count = r.docCount( transformed );
      out.println(count + "\t" + q);
    }
    r.close();
  }

  public static void main(String[] args) throws Exception {
    run(args, System.out);
  }
}
