// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.geometric;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.galagosearch.core.parse.NumberedDocument;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.util.CallTable;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.parse.NumberedDocument")
public class TestGeometricIndexQuerier implements Processor<NumberedDocument> {

  GeometricIndex indexer;
  Parameters queries;
  long frequency;

  public TestGeometricIndexQuerier(TupleFlowParameters parameters) throws Exception {
    indexer = new GeometricIndex(parameters);
    frequency = parameters.getXML().get("queryFreq", 501);
    queries = new Parameters(new File(parameters.getXML().get("queryFile")));
  }

  public void process(NumberedDocument object) throws IOException {
    indexer.process(object);

    if (indexer.globalDocumentCount % frequency == 0) {
      try {
        runQueries();
      } catch (Exception ex) {
        Logger.getLogger(TestGeometricIndexQuerier.class.getName()).log(Level.SEVERE, null, "Failed to run queries\n" + ex.toString());
      }
    }
  }

  public void close() throws IOException {
    indexer.close();
  }

  private void runQueries() throws Exception {
    // read in parameters
    List<Parameters.Value> qs = queries.list("query");

    // copy the parameters for internal use
    //   remove parameters that are used here
    Parameters internalParameters = queries.clone();
    internalParameters.set("query", "");
    internalParameters.set("print_calls", "");

    // for each query, run it, get the results, print in TREC format
    int index = 0;
    for (Parameters.Value query : qs) {

      String queryText = query.get("text");

      Node root = StructuredQuery.parse(queryText);
      Node transformed = indexer.transformQuery(root, "all");

      if (queries.get("printTransformation", true)) {
        System.err.println("Input:" + queryText);
        System.err.println("Parsed:" + root.toString());
        System.err.println("Transformed:" + transformed.toString());
      }

      ScoredDocument[] results = indexer.runQuery(transformed, internalParameters);
      for (int i = 0; i < results.length; i++) {
        double score = results[i].score;
        int rank = i + 1;

        System.out.format("%s Q0 %s %d %s galago\n", query.get("number"), results[i].documentName, rank,
                formatScore(score));
      }
      index++;
      if (queries.get("print_calls", "false").equals("true")) {
        CallTable.print(System.err, Integer.toString(index));
      }
      CallTable.reset();
    }
  }

  public static String formatScore(double score) {
    double difference = Math.abs(score - (int) score);

    if (difference < 0.00001) {
      return Integer.toString((int) score);
    }
    return String.format("%10.8f", score);
  }

}
