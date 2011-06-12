// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.query.SimpleQuery;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.structured.FeatureFactory;
import org.galagosearch.core.retrieval.structured.RankedFeatureFactory;
import org.galagosearch.core.retrieval.structured.StructuredIterator;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.Utility;

/**
 * This class allows searching over a set of Retrievals
 *  - Should this be restricted to a set of StructuredRetievals / StructuredRetrievalProxies ?
 *
 * @author sjh
 */
public class MultiRetrieval implements Retrieval {

  HashMap<String, ArrayList<Retrieval>> retrievals;
  HashMap<String, Parameters> retrievalStatistics;
  HashMap<String, Parameters> retrievalParts;
  HashMap<String, FeatureFactory> featureFactories;
  // for asynchronous retrieval
  Thread runner;
  Node root = null;
  Parameters queryParams;
  List<ScoredDocument> queryResults;
  List<String> errors;

  public MultiRetrieval(HashMap<String, ArrayList<Retrieval>> indexes, Parameters p) throws Exception {

    this.retrievals = indexes;
    initRetrieval(p);
  }

  public void close() throws IOException {
    for (String desc : retrievals.keySet()) {
      ArrayList<Retrieval> collGroup = retrievals.get(desc);
      for (Retrieval r : collGroup) {
        r.close();
      }
    }
  }

  // function accumulates statistics accross index subset
  public Parameters getRetrievalStatistics() throws IOException {
    return getRetrievalStatistics("all");
  }

  public Parameters getRetrievalStatistics(String retGroup) throws IOException {
    System.err.printf("MR checking for group %s (%b)\n", retGroup, retrievalStatistics.containsKey(retGroup));
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

  /**
   *
   * Runs a query across all retrieval objects
   *
   * @param query
   * @param parameters
   * @return
   * @throws Exception
   */
  public ScoredDocument[] runRankedQuery(Node root, Parameters parameters) throws Exception {
    this.root = root;
    String retrievalGroup = parameters.get("retrievalGroup", "all");
    if (!retrievals.containsKey(retrievalGroup)) {
      // this should fail nicely
      // Print a fail, then return null
      throw new Exception("Unable to load id '" + retrievalGroup + "' for query '" + root.toString() + "'");
    }
    ArrayList<Retrieval> subset = retrievals.get(retrievalGroup);
    Parameters shardTemplate = parameters.clone();

    List<ScoredDocument> queryResultCollector = new ArrayList<ScoredDocument>();
    List<String> errorCollector = new ArrayList();

    // Asynchronous retrieval
    String indexId = parameters.get("indexId", "0");
    System.err.printf("Distributing query: %s\n", this.root.toString());
    for (int i = 0; i < subset.size(); i++) {
      Parameters shardParams = shardTemplate.clone();
      shardParams.set("indexId", indexId + "." + Integer.toString(i));
      Retrieval r = subset.get(i);
      r.runAsynchronousQuery(this.root, shardParams, queryResultCollector, errorCollector);
    }

    // Wait for a finished list
    for (Retrieval r : subset) {
      r.waitForAsynchronousQuery();
    }

    if (errorCollector.size() > 0) {
      System.err.println( "Failed to run: " + root.toString() );
      // we do not want to return partial or erroneous results.
      return new ScoredDocument[0];
    }

    
    // sort the results and invert (sort is inverted)
    Collections.sort(queryResultCollector, Collections.reverseOrder());

    // get the best {requested} results
    int requested = (int) parameters.get("requested", 1000);

    return queryResultCollector.subList(0, Math.min(queryResultCollector.size(), requested)).toArray(new ScoredDocument[0]);
  }

  public void runAsynchronousQuery(Node query, Parameters parameters, List<ScoredDocument> queryResults, List<String> errors) throws Exception {
    this.root = query;
    this.queryParams = parameters;
    this.queryResults = queryResults;
    this.errors = errors;

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
      ScoredDocument[] results = runRankedQuery(root, queryParams);

      // Now add it to the output structure, but synchronously
      synchronized (queryResults) {
        queryResults.addAll(Arrays.asList(results));
      }
    } catch (Exception e) {
      // TODO: use logger here
      System.err.println("MultiRetrieval ERROR RETRIEVING: " + e);
      e.printStackTrace(System.err);
      synchronized (errors) {
        errors.add(e.toString());
      }
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

  public Node transformRankedQuery(Node queryTree, String retrievalGroup) throws Exception {
    FeatureFactory ff = featureFactories.get(retrievalGroup);
    List<Traversal> traversals = ff.getTraversals(this);
    for (Traversal traversal : traversals) {
      queryTree = StructuredQuery.copy(traversal, queryTree);
    }
    return queryTree;
  }

  private void initRetrieval(Parameters externalParameters) throws IOException {
    retrievalStatistics = new HashMap();
    retrievalParts = new HashMap();
    featureFactories = new HashMap();
    Parameters partSet;
    Parameters statsSet;
    for (String retGroup : retrievals.keySet()) {
      ArrayList<Parameters> stats = new ArrayList();
      ArrayList<Parameters> parts = new ArrayList();
      for (Retrieval r : retrievals.get(retGroup)) {
        statsSet = r.getRetrievalStatistics(retGroup);
        stats.add(statsSet);
        partSet = r.getAvailableParts(retGroup);
        parts.add(partSet);
      }
      System.err.printf("Merging components for RG %s\n", retGroup);
      partSet = mergeParts(parts);
      retrievalParts.put(retGroup, partSet);

      statsSet = mergeStats(stats, partSet);
      statsSet.add("traversals", externalParameters.list("traversals"));
      statsSet.add("operators", externalParameters.list("operators"));
      System.err.printf("After adding external parameters: %s\n", statsSet.toString());

      retrievalStatistics.put(retGroup, statsSet);
      retrievalStatistics.get(retGroup).add("retrievalGroup", retGroup);
      retrievalStatistics.get(retGroup).add("traversals/traversal/class", "org.galagosearch.core.retrieval.traversal.DetermineCollectionProbabilities");

      featureFactories.put(retGroup, new RankedFeatureFactory(retrievalStatistics.get(retGroup)));
    }
  }

  // this function accumulates statistics collected from the subordinate retrievals
  private Parameters mergeStats(List<Parameters> ps, Parameters parts) {
    long cl = 0;
    long dc = 0;
    for (Parameters p : ps) {
      cl += Long.parseLong(p.get("collectionLength"));
      dc += Long.parseLong(p.get("documentCount"));
    }

    Parameters out = new Parameters();
    out.set("collectionLength", Long.toString(cl));
    out.set("documentCount", Long.toString(dc));

    for (String part : parts.stringList("part")) {
      long pcl = 0;
      long pdc = 0;
      for (Parameters p : ps) {
        pcl += Long.parseLong(p.get("collectionLength"));
        pdc += Long.parseLong(p.get("documentCount"));
      }
      out.set(part + "/collectionLength", Long.toString(pcl));
      out.set(part + "/documentCount", Long.toString(pdc));
    }

    System.err.printf("After merging, stats are: %s\n", out);
    return out;

  }

  // This takes the intersection of parts from constituent retrievals, and determines which
  // part/operator pairs are ok to search on given the current retrievalGroup. We assume that
  // a part is valid if it has at least one usable operator, and an operator is usable if the
  // iteratorClass that implements it is the same across all constituents under a given part.
  private Parameters mergeParts(List<Parameters> ps) {

    // First get all iteratorClasses for all part/operator pairs.
    HashMap<String, HashMap<String, HashSet<String>>> handlers = new HashMap<String, HashMap<String, HashSet<String>>>();
    for (Parameters p : ps) {
      System.err.printf("merge candidate: %s\n", p);
      ArrayList<ArrayList<String>> paths = p.flatten();
      for (ArrayList<String> path : paths) {
        if (path.get(0).equals("nodeType")) {
          if (!handlers.containsKey(path.get(1))) {
            handlers.put(path.get(1), new HashMap<String, HashSet<String>>());
          }
          HashMap<String, HashSet<String>> map = handlers.get(path.get(1));
          if (!map.containsKey(path.get(2))) {
            map.put(path.get(2), new HashSet<String>());
          }
          map.get(path.get(2)).add(path.get(3));
        }
      }
    }

    // Ok, now any handler path that has more than 1 iteratorClass has to be tossed to keep consistency
    // during retrieval
    ArrayList<ArrayList<String>> remainingHandlers = new ArrayList<ArrayList<String>>();
    for (String partName : handlers.keySet()) {
      HashMap<String, HashSet<String>> map = handlers.get(partName);
      for (String operator : map.keySet()) {
        if (map.get(operator).size() == 1) {
          // Only one type of iteratorClass, so this operator's good
          ArrayList<String> good = new ArrayList<String>();
          good.add(partName);
          good.add(operator);
          good.add(map.get(operator).toArray(new String[0])[0]);
          System.err.printf("retaining operator: %s\n", Utility.join(good.toArray(new String[0]), ","));
          remainingHandlers.add(good);
        } else {
          System.err.printf("Operator %s has %d operators - dropping. Set is [%s]\n", operator, map.get(operator).size(),
                  Utility.join(map.get(operator).toArray(new String[0])));
        }
      }
    }

    // Now use the remainingHandlers to determine what goes in the merged
    // parameter set
    Parameters intersection = new Parameters();
    for (ArrayList<String> path : remainingHandlers) {
      // Add the part
      intersection.add("part", path.get(0));
      // Now add the nodeType
      intersection.add("nodeType/" + path.get(0) + "/" + path.get(1), path.get(2));
    }
    System.err.printf("After merging, parts are: %s\n", intersection);
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

    System.err.printf("Distributing xcount: %s\n", countNode.toString());
    
    ArrayList<Retrieval> selected = retrievals.get(retrievalGroup);
    long count = 0;
    for (Retrieval r : selected) {
      count += r.xCount(nodeString);
    }
    return count;
  }

  public long docCount(String nodeString) throws Exception {
    Node countNode = StructuredQuery.parse(nodeString);
    return docCount(countNode);
  }

  /**
   * Note that this assumes the retrieval objects involved in the group
   * contain mutually exclusive subcollections. If you're doing PAC-search
   * or another non-disjoint subset retrieval model, look out.
   */
  public long docCount(Node countNode) throws Exception {
    Parameters parameters = countNode.getParameters();
    String nodeString = countNode.toString();
    String retrievalGroup = parameters.get("retrievalGroup", "all");
    if (!retrievals.containsKey(retrievalGroup)) {
      // this should fail nicely
      // Print a fail, then return null
      throw new Exception("Unable to load id '" + retrievalGroup + "' for query '" + nodeString + "'");
    }
    ArrayList<Retrieval> selected = retrievals.get(retrievalGroup);
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
      if (!parts.containsKey("nodeType/" + partName + "/" + operator)) {
        throw new IOException("The index has no iterator for the operator '" + operator + "'");
      }
      String iteratorClass = parts.get("nodeType/" + partName + "/" + operator);

      // may need to do some checking here...
      return new NodeType((Class<? extends StructuredIterator>) Class.forName(iteratorClass));
    }
    return null;
  }

  @Override
  public ScoredDocument[] runBooleanQuery(Node root, Parameters parameters) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Node transformBooleanQuery(Node root, String retrievalGroup) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
