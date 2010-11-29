// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.query.SimpleQuery;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.tupleflow.Parameters;

/**
 * 10/7/2010 - Modified for asynchronous execution
 *
 * @author trevor
 * @author irmarc
 */
public class StructuredRetrieval extends Retrieval {

  String indexId;
  StructuredIndex index;
  FeatureFactory featureFactory;
  // these allow asynchronous evaluation
  Thread runner;
  String query;
  Parameters queryParams;
  List<ScoredDocument> queryResults;

  public StructuredRetrieval(StructuredIndex index, Parameters factoryParameters) {
    this.index = index;
    Parameters featureParameters = factoryParameters.clone();
    featureParameters.add("collectionLength", Long.toString(index.getCollectionLength()));
    featureParameters.add("documentCount", Long.toString(index.getDocumentCount()));
    featureFactory = new FeatureFactory(featureParameters);
    runner = null;
  }

  public StructuredRetrieval(String filename, Parameters parameters)
          throws FileNotFoundException, IOException {
    this(new StructuredIndex(filename), parameters);
  }

  public void close() throws IOException {
    index.close();
  }

  public Parameters getRetrievalStatistics() throws Exception {
    Parameters p = new Parameters();
    p.add("collectionLength", Long.toString(index.getCollectionLength()));
    p.add("documentCount", Long.toString(index.getDocumentCount()));
    for (String partName : index.getPartNames()) {
      p.add("part", partName);
    }
    return p;
  }

  /**
   * Evaluates a query.
   *
   * @param query A query tree that has been already transformed with StructuredRetrieval.transformQuery.
   * @param parameters - query parameters (indexId, # requested, query type, transform)
   * @return
   * @throws java.lang.Exception
   */
  public ScoredDocument[] runQuery(String query, Parameters parameters) throws Exception {
    // parse the query
    Node queryTree = parseQuery(query, parameters);

    // transform the query if necessary 
    // (if this is part of some distributed retrieval - transformation may not be necessary)
    if (parameters.get("transform", true)) {
      queryTree = transformQuery(queryTree);
    }

    // construct the query iterators
    ScoreIterator iterator = (ScoreIterator) createIterator(queryTree);
    int requested = (int) parameters.get("requested", 1000);

    // now there should be an iterator at the root of this tree
    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>();

    while (!iterator.isDone()) {
      int document = iterator.nextCandidate();
      int length = index.getLength(document);
      double score = iterator.score(document, length);

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

  /*
   * getArrayResults annotates a queue of scored documents
   * returns an array
   * 
   */
  private ScoredDocument[] getArrayResults(PriorityQueue<ScoredDocument> scores, String indexId) throws IOException {
    ScoredDocument[] results = new ScoredDocument[scores.size()];

    for (int i = scores.size() - 1; i >= 0; i--) {
      results[i] = scores.poll();
      results[i].source = indexId;
      results[i].documentName = getDocumentName(results[i].document);
    }
    return results;
  }

  private String getDocumentName(int document) throws IOException {
    return index.getDocumentName(document);
  }

  private Node parseQuery(String query, Parameters parameters) {
    String queryType = parameters.get("queryType", "complex");

    if (queryType.equals("simple")) {
      return SimpleQuery.parseTree(query);
    }

    return StructuredQuery.parse(query);
  }

  private StructuredIterator createIterator(Node node) throws Exception {
    ArrayList<StructuredIterator> internalIterators = new ArrayList<StructuredIterator>();

    for (Node internalNode : node.getInternalNodes()) {
      StructuredIterator internalIterator = createIterator(internalNode);
      internalIterators.add(internalIterator);
    }

    StructuredIterator iterator = index.getIterator(node);
    if (iterator == null) {
      iterator = featureFactory.getIterator(node, internalIterators);
    }

    return iterator;
  }

  private Node transformQuery(Node queryTree) throws Exception {
    List<Traversal> traversals = featureFactory.getTraversals(this);
    for (Traversal traversal : traversals) {
      queryTree = StructuredQuery.copy(traversal, queryTree);
    }
    return queryTree;
  }

  // NASTY HACKS THAT NEED TO BE FIXED
  public NodeType getNodeType(Node node) throws Exception {
    NodeType nodeType = index.getNodeType(node);
    if (nodeType == null) {
      nodeType = featureFactory.getNodeType(node);
    }
    return nodeType;
  }

  public StructuredIndex getIndex() {
    return index;
  }
}
