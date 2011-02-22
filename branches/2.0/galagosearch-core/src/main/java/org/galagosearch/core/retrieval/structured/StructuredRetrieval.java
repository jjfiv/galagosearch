// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import gnu.trove.TObjectDoubleHashMap;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.TreeMap;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.query.SimpleQuery;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.core.util.CallTable;
import org.galagosearch.tupleflow.Parameters;

/**
 * 10/7/2010 - Modified for asynchronous execution
 *
 * @author trevor
 * @author irmarc
 */
public class StructuredRetrieval extends Retrieval {

  protected String indexId;
  protected StructuredIndex index;
  protected FeatureFactory featureFactory;
  // these allow asynchronous evaluation
  protected Thread runner;
  protected Node query;
  protected Parameters queryParams;
  protected List<ScoredDocument> queryResults;

  public StructuredRetrieval(StructuredIndex index, Parameters factoryParameters) throws IOException {
    this.index = index;

    Parameters featureParameters = factoryParameters.clone();
    Parameters indexStats = getRetrievalStatistics("all");
    featureParameters.add("collectionLength", indexStats.get("collectionLength"));
    featureParameters.add("documentCount", indexStats.get("documentCount"));
    featureParameters.add("retrievalGroup", "all"); // the value wont matter here
    featureFactory = new DocumentOrderedFeatureFactory(featureParameters);
    runner = null;
  }

  public StructuredRetrieval(String filename, Parameters parameters)
          throws FileNotFoundException, IOException {
    this(new StructuredIndex(filename), parameters);
  }

  public void close() throws IOException {
    index.close();
  }

  /*
   * <parameters>
   *  <collectionLength>cl<collectionLength>
   *  <documentCount>dc<documentCount>
   * </parameters>
   */
  public Parameters getRetrievalStatistics(String _retGroup) throws IOException {
    Parameters p = new Parameters();
    p.add("collectionLength", Long.toString(index.getCollectionLength()));
    p.add("documentCount", Long.toString(index.getDocumentCount()));
    return p;
  }
  /*
   * <parameters>
   *  <part>
   *   (partName)+
   *  </part>
   *  <nodeType>
   *   <(partName)_(nodeType)>(class)</(partName)_(nodeType)>
   *  </nodeType>
   * </parameters>
   */

  public Parameters getAvailableParts(String _retGroup) throws IOException {
    Parameters p = new Parameters();
    for (String partName : index.getPartNames()) {
      p.add("part", partName);

      Map<String, NodeType> nodeTypes = index.getPartNodeTypes(partName);
      for (String nodeType : nodeTypes.keySet()) {
        p.add("nodeType/" + partName + "/" + nodeType, nodeTypes.get(nodeType).getIteratorClass().getName());
      }
    }
    return p;
  }

  /**
   * Evaluates a query using identifier-at-a-time evaluation.
   *
   * @param query A query tree that has been already transformed with StructuredRetrieval.transformQuery.
   * @param parameters - query parameters (indexId, # requested, query type, transform)
   * @return
   * @throws java.lang.Exception
   */
  public ScoredDocument[] runQuery(Node queryTree, Parameters parameters) throws Exception {

    // construct the query iterators
      DocumentOrderedScoreIterator iterator = (DocumentOrderedScoreIterator) createIterator(queryTree);
    int requested = (int) parameters.get("requested", 1000);

    // now there should be an iterator at the root of this tree
    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>();
    NumberedDocumentDataIterator lengthsIterator = index.getDocumentLengthsIterator();
    
    while (!iterator.isDone()) {
      int document = iterator.currentCandidate();
      lengthsIterator.skipTo(document);
      int length = lengthsIterator.getDocumentData().textLength;
      // This context is shared among all scorers
      iterator.setScoringContext(document, length);
      double score = iterator.score();
      CallTable.increment("scored");
      if (queue.size() <= requested || queue.peek().score < score) {
        ScoredDocument scoredDocument = new ScoredDocument(document, score);
        queue.add(scoredDocument);

        if (queue.size() > requested) {
          queue.poll();
        }
      }

      iterator.movePast(document);
    }

    String indexId = parameters.get("indexId", "0");
    return getArrayResults(queue, indexId);
  }

  /**
   *
   * @param query - query to be evaluated
   * @param parameters - query parameters (indexId, # requested, query type, transform, retrievalGroup)
   * @param queryResults - object that will contain the results
   * @throws Exception
   */
  public void runAsynchronousQuery(Node query, Parameters parameters, List<ScoredDocument> queryResults) throws Exception {
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

  /**
   * Evaluates a query using identifier-at-a-time evaluation.
   *  - allowing user to sweep accross parameters specified within the query
   *
   * @param query A query tree that has been already transformed with StructuredRetrieval.transformQuery.
   * @param parameters - query parameters (indexId, # requested, query type, transform)
   * @return
   * @throws java.lang.Exception
   */
  public ScoredDocument[] runParameterSweep(Node queryTree, Parameters parameters) throws Exception {

    // construct the query iterators
    DocumentOrderedScoreIterator iterator = (DocumentOrderedScoreIterator) createIterator(queryTree);
    int requested = (int) parameters.get("requested", 1000);

    // now there should be an iterator at the root of this tree
    HashMap<String, PriorityQueue<ScoredDocument>> queues = new HashMap();
    NumberedDocumentDataIterator lengthsIterator = index.getDocumentLengthsIterator();

    while (!iterator.isDone()) {
      int document = iterator.currentCandidate();
      lengthsIterator.skipTo(document);
      int length = lengthsIterator.getDocumentData().textLength;

      // This context is shared among all scorers
      iterator.setScoringContext(document, length);
      TObjectDoubleHashMap<String> scores = iterator.parameterSweepScore();

      for (String params : scores.keys( new String[0] )) {
        if (!queues.containsKey(params)) {
          queues.put(params, new PriorityQueue());
        }
        ScoredDocument scoredDocument = new ScoredDocument(document, scores.get(params));
        PriorityQueue q = queues.get(params);
        q.add(scoredDocument);
        if (q.size() > requested) {
          q.poll();
        }
      }

      iterator.movePast(document);
    }

    String indexId = parameters.get("indexId", "0");
    return getPSArrayResults(queues, indexId);
  }

  /*
   * getArrayResults annotates a queue of scored documents
   * returns an array
   *
   */
  protected ScoredDocument[] getArrayResults(PriorityQueue<ScoredDocument> scores, String indexId) throws IOException {
    ScoredDocument[] results = new ScoredDocument[scores.size()];

    TreeMap<Integer, Integer> docIds = new TreeMap();

    for (int i = scores.size() - 1; i >= 0; i--) {
      results[i] = scores.poll();
      results[i].source = indexId;
      results[i].rank = i + 1;
      //results[i].documentName = getDocumentName(results[i].identifier);
      docIds.put(results[i].document, i);
    }

    NumberedDocumentDataIterator iterator = index.getDocumentNamesIterator();
    for (int document : docIds.keySet()) {
      iterator.skipTo(document);
      NumberedDocumentData ndd = iterator.getDocumentData();
      if (ndd.number == document) {
        results[docIds.get(document)].documentName = ndd.identifier;
      } else {
        results[docIds.get(document)].documentName = "DOCUMENT_NAME_NOT_FOUND";
      }
    }

    return results;
  }

  /*
   * getArrayResults annotates a queue of scored documents
   * returns an array
   *
   */
  protected ScoredDocument[] getPSArrayResults(Map<String, PriorityQueue<ScoredDocument>> scoreMap, String indexId) throws IOException {
    ArrayList<ScoredDocument> results = new ArrayList();

    for (String params : scoreMap.keySet()) {
      PriorityQueue<ScoredDocument> queue = scoreMap.get(params);
      ScoredDocument[] qresults = new ScoredDocument[queue.size()];
      for (int i = queue.size() - 1; i >= 0; i--) {
        qresults[i] = queue.poll();
        qresults[i].rank = i + 1;
        qresults[i].source = indexId;
        qresults[i].documentName = getDocumentName(qresults[i].document);
        qresults[i].params = params;
      }
      results.addAll(Arrays.asList(qresults));
    }

    return results.toArray(new ScoredDocument[0]);
  }

  protected String getDocumentName(int document) throws IOException {
    return index.getDocumentName(document);
  }

  protected Node parseQuery(String query, Parameters parameters) {
    String queryType = parameters.get("queryType", "complex");

    if (queryType.equals("simple")) {
      return SimpleQuery.parseTree(query);
    }

    return StructuredQuery.parse(query);
  }

  public StructuredIterator createIterator(Node node) throws Exception {
    ArrayList<StructuredIterator> internalIterators = new ArrayList<StructuredIterator>();
    StructuredIterator iterator;
    try {
      for (Node internalNode : node.getInternalNodes()) {
        StructuredIterator internalIterator = createIterator(internalNode);
        internalIterators.add(internalIterator);
      }

      iterator = index.getIterator(node);
      if (iterator == null) {
        iterator = featureFactory.getIterator(node, internalIterators);
      }
    } catch (Exception e) {
      throw e;
    }
    return iterator;
  }

  public Node transformQuery(Node queryTree, String retrievalGroup) throws Exception {
    List<Traversal> traversals = featureFactory.getTraversals(this);
    for (Traversal traversal : traversals) {
      queryTree = StructuredQuery.copy(traversal, queryTree);
    }
    return queryTree;
  }

  /**
   * Returns the number of occurrences of the provided
   * expression. If the expression does not produce a CountIterator
   * as a node type, throws an IllegalArgumentException, since it's not
   * an appropriate input. #text, #ow, and #uw should definitely be ok here.
   *
   * @param nodeString
   * @return Number of times the expression occurs.
   * @throws Exception
   */
  public long xcount(String nodeString) throws Exception {

    // first parse the node
    Node root = StructuredQuery.parse(nodeString);
    return xcount(root);
  }

  public long xcount(Node root) throws Exception {
    StructuredIterator structIterator = index.getIterator(root);
    if (structIterator instanceof CountIterator) {
      CountIterator iterator = (CountIterator) structIterator;
      long count = 0;
      while (!iterator.isDone()) {
        count += iterator.count();
        iterator.next();
      }
      return count;
    } else {
      throw new IllegalArgumentException("Node " + root.toString() + " did not return a counting iterator.");
    }
  }

  public NodeType getNodeType(Node node, String retrievalGroup) throws Exception {
    NodeType nodeType = index.getNodeType(node);
    if (nodeType == null) {
      nodeType = featureFactory.getNodeType(node);
    }
    return nodeType;
  }
}
