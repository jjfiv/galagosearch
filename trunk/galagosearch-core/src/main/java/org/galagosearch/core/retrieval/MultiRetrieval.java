// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.query.SimpleQuery;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.structured.DocumentOrderedFeatureFactory;
import org.galagosearch.core.retrieval.structured.FeatureFactory;
import org.galagosearch.core.retrieval.structured.StructuredIterator;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Parameters.Value;

/**
 * This class allows searching over a set of Retrievals
 *  - Should this be restricted to a set of StructuredRetievals / StructuredRetrievalProxies ?
 *
 * @author sjh
 */
public class MultiRetrieval extends Retrieval {

  protected HashMap<String, Collection<Retrieval>> retrievals;
  protected HashMap<String, Parameters> retrievalStatistics;
  protected HashMap<String, Parameters> retrievalParts;
  protected HashMap<String, FeatureFactory> featureFactories;
  // for asynchronous retrieval
  protected Thread runner;
  protected Node root = null;
  protected Parameters queryParams;
  protected Parameters retrievalParams;
  protected List<ScoredDocument> queryResults;

  public MultiRetrieval(HashMap<String, Collection<Retrieval>> indexes, Parameters p) throws Exception {

    retrievals = indexes;
    retrievalParams = p;
    initRetrieval();
  }

  public void close() throws IOException {
    for (String desc : retrievals.keySet()) {
     Collection<Retrieval> collGroup = retrievals.get(desc);
      for (Retrieval r : collGroup) {
        r.close();
      }
    }
  }

  // function accumulates statistics accross index subset
  public Parameters getRetrievalStatistics(String retGroup) throws IOException {
    if (retrievalStatistics.containsKey(retGroup)) {
      return retrievalStatistics.get(retGroup);
    } else {
      return null;
    }
  }

  public Parameters getAvailableParts(String retGroup) throws IOException {
    if (retrievalParts.containsKey(retGroup)) {
      return retrievalParts.get(retGroup);
    } else {
      return null;
    }
  }

  public StructuredIterator createIterator(Node node) throws Exception {
      throw new UnsupportedOperationException("Semantics to instantiate iterator are unclear");
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
  public ScoredDocument[] runQuery(Node root, Parameters parameters) throws Exception {
    this.root = root;
    String retrievalGroup = parameters.get("retrievalGroup", "all");
    if (!retrievals.containsKey(retrievalGroup)) {
      // this should fail nicely
      // Print a fail, then return null
      throw new Exception("Unable to load id '" + retrievalGroup + "' for query '" + root.toString() + "'");
    }
    Collection<Retrieval> subset = retrievals.get(retrievalGroup);
    List<ScoredDocument> queryResults = new ArrayList<ScoredDocument>();

    Parameters shardTemplate = parameters.clone();

    // Asynchronous retrieval
    String indexId = parameters.get("indexId", "0");

    int i =0;
    for (Retrieval r : subset) {
      Parameters shardParams = shardTemplate.clone();
      shardParams.set("indexId", indexId + "." + Integer.toString(i));
      r.runAsynchronousQuery(this.root, shardParams, queryResults);
      i++;
    }

    // Wait for a finished list
    for (Retrieval r : subset) {
      r.waitForAsynchronousQuery();
    }

    // sort the results
    Collections.sort(queryResults);

    // get the best {requested} results
    int requested = (int) parameters.get("count", 1000);

    return queryResults.subList(0, Math.min(queryResults.size(), requested)).toArray(new ScoredDocument[0]);
  }

  public void runAsynchronousQuery(Node query, Parameters parameters, List<ScoredDocument> queryResults) throws Exception {
    this.root = query;
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
    root = null;
    runner = null;
  }

  public void run() {
    // we haven't got a query to run - return
    if (root == null) {
      return;
    }

    try {
      ScoredDocument[] results = runQuery(root, queryParams);

      // Now add it to the output structure, but synchronously
      synchronized (queryResults) {
        queryResults.addAll(Arrays.asList(results));
      }
    } catch (Exception e) {
      // TODO: use logger here
      System.err.println("ERROR RETRIEVING: " + e.getMessage());
    }
  }

  public ScoredDocument[] runParameterSweep(Node root, Parameters parameters) throws Exception{
    throw new UnsupportedOperationException("Parameter Sweep not yet implemented");
  }

  // private functions
  private Node parseQuery(String query, Parameters parameters) {
    String queryType = parameters.get("queryType", "complex");

    if (queryType.equals("simple")) {
      return SimpleQuery.parseTree(query);
    }

    return StructuredQuery.parse(query);
  }

  public Node transformQuery(Node queryTree, String retrievalGroup) throws Exception {
    FeatureFactory ff = featureFactories.get(retrievalGroup);
    List<Traversal> traversals = ff.getTraversals(this);
    for (Traversal traversal : traversals) {
      queryTree = StructuredQuery.copy(traversal, queryTree);
    }
    return queryTree;
  }

  private void initRetrieval() throws IOException {
    retrievalStatistics = new HashMap();
    retrievalParts = new HashMap();
    featureFactories = new HashMap();

    for (String retGroup : retrievals.keySet()) {
      ArrayList<Parameters> stats = new ArrayList();
      ArrayList<Parameters> parts = new ArrayList();
      for (Retrieval r : retrievals.get(retGroup)) {
        stats.add(r.getRetrievalStatistics(retGroup));
        parts.add(r.getAvailableParts(retGroup));
      }
      retrievalStatistics.put(retGroup, mergeStats(stats));
      retrievalStatistics.get(retGroup).add("retrievalGroup", retGroup);
      retrievalParts.put(retGroup, mergeParts(parts));

      featureFactories.put(retGroup, new DocumentOrderedFeatureFactory(retrievalStatistics.get(retGroup)));
    }
  }

  // this function accumulates statistics collected from the subordinate retrievals
  protected Parameters mergeStats(List<Parameters> ps) {
    long cl = 0;
    long dc = 0;
    for (Parameters p : ps) {
      cl += Long.parseLong(p.get("collectionLength"));
      dc += Long.parseLong(p.get("documentCount"));
    }

    Parameters out = new Parameters();
    out.set("collectionLength", Long.toString(cl));
    out.set("documentCount", Long.toString(dc));

    return out;

  }

  // this function intersects the set of availiable parts
  // ASSUMPTION: part names correspond to unique index part concepts (that could be merged)
  protected Parameters mergeParts(List<Parameters> ps) {
    Parameters intersection = ps.get(0).clone();
    for (String partName : intersection.stringList("part")) {
      for (Parameters p : ps) {
        // if some index does not contain the correct part - delete it and all node Classes
        if (! p.stringList("part").contains(partName)) {
          for(Value v : intersection.list("part")){
            if(v.toString().equals(partName)){
              intersection.list("part").remove(v);
            }
          }
        }
      }
    }
    return intersection;
  }

  /**
   * Currently does this synchronously to make sure it works.
   * We can multi-thread it when we have time.
   */
  public long xCount(String nodeString) throws Exception {
    // For now, grab the parameters from the node itself.
    // Maybe a better option than parsing the query at EVERY level,
    // but at least it works.
    Node countNode = StructuredQuery.parse(nodeString);
    return xCount(countNode);
  }

  public long xCount(Node countNode) throws Exception {
    Parameters parameters = countNode.getParameters();
    String nodeString = countNode.toString();
    String retrievalGroup = parameters.get("retrievalGroup", "all");
    if (!retrievals.containsKey(retrievalGroup)) {
      // this should fail nicely
      // Print a fail, then return null
      throw new Exception("Unable to load id '" + retrievalGroup + "' for query '" + nodeString + "'");
    }
    Collection<Retrieval> selected = retrievals.get(retrievalGroup);
    long count = 0;
    for (Retrieval r : selected) {
      count += r.xCount(nodeString);
    }
    return count;
  }

    /**
   * Currently does this synchronously to make sure it works.
   * We can multi-thread it when we have time.
   */
  public long docCount(String nodeString) throws Exception {
    // For now, grab the parameters from the node itself.
    // Maybe a better option than parsing the query at EVERY level,
    // but at least it works.
    Node countNode = StructuredQuery.parse(nodeString);
    return docCount(countNode);
  }

  public long docCount(Node countNode) throws Exception {
    Parameters parameters = countNode.getParameters();
    String nodeString = countNode.toString();
    String retrievalGroup = parameters.get("retrievalGroup", "all");
    if (!retrievals.containsKey(retrievalGroup)) {
      // this should fail nicely
      // Print a fail, then return null
      throw new Exception("Unable to load id '" + retrievalGroup + "' for query '" + nodeString + "'");
    }
    Collection<Retrieval> selected = retrievals.get(retrievalGroup);
    long count = 0;
    for (Retrieval r : selected) {
      count += r.docCount(nodeString);
    }
    return count;
  }

  public NodeType getNodeType(Node node, String retrievalGroup) throws Exception {
    NodeType nodeType = getIndexNodeType(node, retrievalGroup);
    if (nodeType == null) {
      nodeType = featureFactories.get(retrievalGroup).getNodeType(node);
    }
    return nodeType;
  }

  private NodeType getIndexNodeType(Node node, String retrievalGroup) throws Exception {
    if (node.getParameters().containsKey("part")) {
      Parameters parts = this.getAvailableParts(retrievalGroup);
      String partName = node.getParameters().get("part");

      if (!parts.stringList("part").contains(partName)) {
        throw new IOException("The index has no part named '" + partName + "'");
      }
      String operator = node.getOperator();
      if(! parts.containsKey("nodeType/" + partName + "/" + operator)){
        throw new IOException("The index has no iterator for the operator '" + operator + "'");
      } 
      String iteratorClass = parts.get("nodeType/" + partName + "/" + operator);

      // may need to do some checking here...
      return new NodeType( (Class<? extends StructuredIterator>) Class.forName(iteratorClass));
    }
    return null;
  }
}
