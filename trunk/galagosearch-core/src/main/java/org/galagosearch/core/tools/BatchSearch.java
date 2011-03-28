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
import org.galagosearch.core.util.CallTable;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;
import org.tartarus.snowball.ext.englishStemmer;

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

    // copy the parameters for internal use
    //   remove parameters that are used here
    Parameters internalParameters = parameters.clone();
    internalParameters.set("query", "");
    internalParameters.set("print_calls", "");

    // open index
    Retrieval retrieval = Retrieval.instance(internalParameters);

    // for each query, run it, get the results, print in TREC format
    int index = 0;
    for (Parameters.Value query : queries) {

      String queryText = query.get("text");

      Node root = StructuredQuery.parse(queryText);
      Node transformed = retrieval.transformQuery(root, "all");

      if (parameters.get("printTransformation", false)) {
        System.err.println("Input:" + queryText);
        System.err.println("Parsed:" + root.toString());
        System.err.println("Transformed:" + transformed.toString());
      }

      ScoredDocument[] results = retrieval.runQuery(transformed, internalParameters);
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
    englishStemmer stemmer = null;
    Parameters p = new Parameters(Utility.subarray(args, 1));
    Retrieval r = Retrieval.instance(p);

    String defPart = "";
    Parameters availableParts = r.getAvailableParts("all");
    List<String> available = availableParts.stringList("part");
    if (available.contains("stemmedPostings")) {
      defPart = "stemmedPostings";
      stemmer = new englishStemmer();
    } else if (available.contains("postings")) {
      defPart = "postings";
    }

    long count;
    for (Parameters.Value v : p.list("x")) {
      String q = v.toString();
      // System.err.println(q);
      if (q.contains("#")) {
        count = r.xCount(q);
      } else {
      
        String termOp = "#counts:" + q + ":part=" + defPart + "()";

        if(stemmer != null){
          stemmer.setCurrent(q);
          stemmer.stem();
          String stemmed = stemmer.getCurrent();
          termOp = "#counts:" + stemmed + ":part=" + defPart + "()";
        }

        count = r.xCount(termOp);
      }
      out.println(count + "\t" + q);
    }
    r.close();
  }

  public static void docCount(String[] args, PrintStream out) throws Exception {
    Parameters p = new Parameters(Utility.subarray(args, 1));
    Retrieval r = Retrieval.instance(p);

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


  public static void queryLeafCounter(String[] args, PrintStream out) throws Exception {
    Parameters p = new Parameters(Utility.subarray(args, 1));
    Retrieval r = Retrieval.instance(p);

    List<Parameters.Value> queries = p.list("query");

    for (Parameters.Value query : queries) {
      String queryText = query.get("text");
      try{
        Node root = StructuredQuery.parse(queryText);
        Node transformed = r.transformQuery(root, "all");
        String output = traverseXCount(transformed, r, query.get("number"));
        out.print(output);
      } catch (Exception e){
        // System.err.println( "Died on :" + queryText );
        // ignored
      }
    }
  }

  private static String traverseXCount(Node n, Retrieval r, String prefix) throws Exception{
    // if we have a feature - we want to count the child
    if( n.getOperator().equals( "feature" ) ){
      Node child = n.getInternalNodes().get(0);
      long c = r.xCount(child);
      return (c + "\t" + prefix + "\t" + getText(child) + "\n");
    } else {
      StringBuilder sb = new StringBuilder();
      for(Node child : n.getInternalNodes()){
        sb.append(traverseXCount(child, r, prefix));
      }
      return sb.toString();
    }
  }

  private static String getText(Node n){
    if(n.getOperator().startsWith("count") ||
            n.getOperator().startsWith("extent")){
      return n.getDefaultParameter();
    } else {
      StringBuilder sb = new StringBuilder();
      for(Node c : n.getInternalNodes()){
        sb.append( getText(c) );
        sb.append( " " );
      }
      return sb.toString();
    }
  }

  public static void main(String[] args) throws Exception {
    run(args, System.out);
  }
}
