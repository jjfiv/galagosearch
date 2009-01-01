// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.structured;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class StructuredRetrieval extends Retrieval {
    StructuredIndex index;
    FeatureFactory featureFactory;

    /**
     * Creates a new instance of StructuredRetrieval
     */
    public StructuredRetrieval(String filename) throws FileNotFoundException, IOException {
        index = new StructuredIndex(filename);
        Parameters featureParameters = new Parameters();
        featureParameters.add("collectionLength", Long.toString(index.getCollectionLength()));
        featureParameters.add("documentCount", Long.toString(index.getDocumentCount()));
        featureFactory = new FeatureFactory(featureParameters);
    }

    public ScoredDocument[] getArrayResults(PriorityQueue<ScoredDocument> scores) {
        ScoredDocument[] results = new ScoredDocument[scores.size()];

        for (int i = scores.size() - 1; i >= 0; i--) {
            results[i] = scores.poll();
        }

        return results;
    }
    
    public NodeType getNodeType(Node node) throws Exception {
        NodeType nodeType = index.getNodeType(node);
        if (nodeType == null) {
            nodeType = featureFactory.getNodeType(node);
        }
        return nodeType;
    }
    
    public StructuredIterator createIterator(Node node) throws Exception {
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
    
    public ScoredDocument[] runQuery(Node queryTree, int requested) throws Exception {
        // construct the query iterators
        ScoreIterator iterator = (ScoreIterator) createIterator(queryTree);

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

        return getArrayResults(queue);
    }

    public String getDocument(int document) {
        return index.getDocument(document);
    }

    public void close() throws IOException {
        index.close();
    }

    public static void main(String[] args) throws Exception {
        // read in parameters
        Parameters parameters = new Parameters(args);
        List<Parameters.Value> queries = parameters.list("query");

        // open index
        StructuredRetrieval retrieval = new StructuredRetrieval(parameters.get("index"));

        // record results requested
        int requested = (int) parameters.get("count", 1000);

        // for each query, run it, get the results, look up the docnos, print in TREC format
        for (Parameters.Value query : queries) {
            String queryText = query.get("text");
            Node queryTree = StructuredQuery.parse(queryText);

            ScoredDocument[] results = retrieval.runQuery(queryTree, requested);

            for (int i = 0; i < results.length; i++) {
                String document = retrieval.getDocument(results[i].document);
                double score = results[i].score;
                int rank = i + 1;

                System.out.format("%s Q0 %s %s %10.8f galago\n", query.get("number"),
                                  document, rank, score);
            }
        }
    }
}
