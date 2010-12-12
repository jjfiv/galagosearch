// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
    Node query;
    Parameters queryParams;
    List<ScoredDocument> queryResults;

    public StructuredRetrieval(StructuredIndex index, Parameters factoryParameters) throws IOException {
        this.index = index;

        Parameters featureParameters = factoryParameters.clone();
        Parameters indexStats = getRetrievalStatistics("all");
        featureParameters.add("collectionLength", indexStats.get("collectionLength"));
        featureParameters.add("documentCount", indexStats.get("documentCount"));
        featureParameters.add("retrievalGroup", "all"); // the value wont matter here
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
    public Parameters getAvailiableParts(String _retGroup) throws IOException {
        Parameters p = new Parameters();
        for (String partName : index.getPartNames()) {
          p.add("part", partName);

          Map<String, NodeType> nodeTypes = index.getPartNodeTypes(partName);
          for(String nodeType : nodeTypes.keySet()){
            p.add("nodeType/" + partName + "/" + nodeType, nodeTypes.get(nodeType).getIteratorClass().getName());
          }
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
    public ScoredDocument[] runQuery(Node queryTree, Parameters parameters) throws Exception {

        // construct the query iterators
        ScoreIterator iterator = (ScoreIterator) createIterator(queryTree);
        int requested = (int) parameters.get("requested", 1000);

        // now there should be an iterator at the root of this tree
        PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>();
        NumberedDocumentDataIterator lengthsIterator = index.getDocumentLengthsIterator();

        while (!iterator.isDone()) {
            int document = iterator.nextCandidate();
            lengthsIterator.skipTo( document );
            int length = lengthsIterator.getDocumentData().textLength;
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
                iterator.nextDocument();
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
