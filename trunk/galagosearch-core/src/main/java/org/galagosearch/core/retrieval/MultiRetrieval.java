// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.SimpleQuery;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.structured.FeatureFactory;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Parameters.Value;

/**
 * This class allows searching over a set of Retrievals
 *  - Should this be restricted to a set of StructuredRetievals / StructuredRetrievalProxies ?
 *
 * @author sjh
 */
public class MultiRetrieval extends Retrieval {

  HashMap<String, ArrayList<Retrieval>> retrievals;
  // for asynchronous retrieval
  Thread runner;
  String query;
  Parameters queryParams;
  List<ScoredDocument> queryResults;

  public MultiRetrieval(Parameters p) throws Exception {
    this.retrievals = new HashMap<String, ArrayList<Retrieval>>();
    this.retrievals.put("all", new ArrayList<Retrieval>());

    // Load up the indexes
    String id, path;
    List<Parameters.Value> indexes = p.list("index");
    for (Parameters.Value value : indexes) {
      id = "all";
      if (value.containsKey("path")) {
        path = value.get("path").toString();
        if (value.containsKey("id")) {
          id = value.get("id").toString();
        }
      } else {
        path = value.toString();
      }
      if (!retrievals.containsKey(id)) {
        retrievals.put(id, new ArrayList<Retrieval>());
      }

      try {
        Retrieval r = Retrieval.instance(path, p);
        retrievals.get(id).add(r);
        if (!id.equals("all")) {
          retrievals.get("all").add(r); // Always put it in default as well
        }
      } catch (Exception e) {
        System.err.println("Unable to load index (" + id + ") at path " + path + ": " + e.getMessage());
      }
    }

    initRetrievalStatistics();
  }

  public void close() throws IOException {
    for (String desc : retrievals.keySet()) {
      ArrayList<Retrieval> collGroup = retrievals.get(desc);
      for (Retrieval r : collGroup) {
        r.close();
      }
    }
  }
  HashMap<String, Parameters> retrievalStatistics = new HashMap();

  private void initRetrievalStatistics() throws IOException {
    for (String retGroup : retrievals.keySet()) {
      ArrayList<Parameters> stats = new ArrayList();
      for (Retrieval r : retrievals.get(retGroup)) {
        stats.add(r.getRetrievalStatistics());
      }
      retrievalStatistics.put(retGroup, mergeParameters(stats));
    }
  }

  private Parameters mergeParameters(List<Parameters> ps) {

    // first collection stats
    long cl = 0;
    long dc = 0;
    for (Parameters p : ps) {
      cl += Long.parseLong(p.get("collectionLength"));
      dc += Long.parseLong(p.get("documentCount"));
    }

    // next available parts
    Parameters intersection = ps.get(0).clone();
    for (String partName : intersection.listKeys()) {
      // ignore the collection stats from this object
      if (partName == "collectionLength" || partName == "documentCount") {
        continue;
      }
      for(Parameters p : ps){
        // if some index does not contain the correct part:
        if( ! p.containsKey(partName) ){
          intersection.value().map().remove(partName);
        }
      }
    }

    intersection.set("collectionLength", Long.toString(cl));
    intersection.set("documentCount", Long.toString(dc));

    return intersection;
  }

  // function accumulates statistics accross index subset
  public Parameters getRetrievalStatistics() throws IOException {
    return retrievalStatistics.get("all");
  }

  /**
   *
   * Runs a query across all retrieval objects
   *
   * @param query
   * @param parameters
   * @return
   * @throws Exception
   */
  public ScoredDocument[] runQuery(String query, Parameters parameters) throws Exception {
    String retrievalGroup = parameters.get("retrievalGroup", "all");
    if (!retrievals.containsKey(retrievalGroup)) {
      // this should fail nicely
      // Print a fail, then return null
      throw new Exception("Unable to load id '" + retrievalGroup + "' for query '" + query + "'");
    }
    ArrayList<Retrieval> subset = retrievals.get(retrievalGroup);
    List<ScoredDocument> queryResults = new ArrayList<ScoredDocument>();

    Parameters shardTemplate = parameters.clone();

    if (parameters.get("transform", true)) {
      Node queryRoot = parseQuery(query, parameters);
      queryRoot = transformQuery(queryRoot);
      query = queryRoot.toString();
      shardTemplate.set("transform", "false");
    }

    // Asynchronous retrieval
    String indexId = parameters.get("indexId", "0");

    for (int i = 0; i < subset.size(); i++) {
      Parameters shardParams = shardTemplate.clone();
      shardParams.set("indexId", indexId + "." + Integer.toString(i));
      Retrieval r = subset.get(i);
      r.runAsynchronousQuery(query, shardParams, queryResults);
    }

    // Wait for a finished list
    for (Retrieval r : subset) {
      r.waitForAsynchronousQuery();
    }

    // sort the results
    Collections.sort(queryResults);

    // get the best {requested} results
    int requested = (int) parameters.get("requested", 1000);
    return queryResults.subList(0, requested).toArray(new ScoredDocument[0]);
  }

  public void runAsynchronousQuery(String query, Parameters parameters, List<ScoredDocument> queryResults) throws Exception {
    this.query = query;
    this.queryParams = parameters;
    this.queryResults = queryResults;

    runner = new Thread(this);
    runner.start();
  }

  public void waitForAsynchronousQuery() throws InterruptedException {
    this.join();
  }

  // Finish and clean up
  public void join() throws InterruptedException {
    if (runner != null) {
      runner.join();
    }
    query = null;
    runner = null;
  }

  public void run() {
    // we haven't got a query to run - return
    if (query == null) {
      return;
    }

    try {
      ScoredDocument[] results = runQuery(query, queryParams);

      // Now add it to the output structure, but synchronously
      synchronized (queryResults) {
        queryResults.addAll(Arrays.asList(results));
      }
    } catch (Exception e) {
      // TODO: use logger here
      System.err.println("ERROR RETRIEVING: " + e.getMessage());
    }
  }

  // private functions
  private Node parseQuery(String query, Parameters parameters) {
    String queryType = parameters.get("queryType", "complex");

    if (queryType.equals("simple")) {
      return SimpleQuery.parseTree(query);
    }

    return StructuredQuery.parse(query);
  }

  private Node transformQuery(Node queryTree) throws Exception {
    throw new Exception("unimplemented");
//    List<Traversal> traversals = featureFactory.getTraversals(this);
//    for (Traversal traversal : traversals) {
//     queryTree = StructuredQuery.copy(traversal, queryTree);
//   }
//   return queryTree;
  }

  /**
   * Currently does this synchronously to make sure it works.
   * We can multi-thread it when we have time.
   */
  @Override
  public long xcount(String nodeString) throws Exception {
    // For now, grab the parameters from the node itself.
    // Maybe a better option than parsing the query at EVERY level,
    // but at least it works.
    Node countNode = StructuredQuery.parse(nodeString);
    Parameters parameters = countNode.getParameters();
    String retrievalGroup = parameters.get("retrievalGroup", "all");
    if (!retrievals.containsKey(retrievalGroup)) {
      // this should fail nicely
      // Print a fail, then return null
      throw new Exception("Unable to load id '" + retrievalGroup + "' for query '" + query + "'");
    }
    ArrayList<Retrieval> selected = retrievals.get(retrievalGroup);
    long count = 0;
    for (Retrieval r : selected) {
      count += r.xcount(nodeString);
    }
    return count;
  }
}
