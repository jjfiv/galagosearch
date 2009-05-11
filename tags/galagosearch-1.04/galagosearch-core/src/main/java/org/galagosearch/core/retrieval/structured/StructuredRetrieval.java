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
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class StructuredRetrieval extends Retrieval {
    StructuredIndex index;
    FeatureFactory featureFactory;

    public StructuredRetrieval(StructuredIndex index, Parameters factoryParameters) {
        this.index = index;
        Parameters featureParameters = factoryParameters.clone();
        featureParameters.add("collectionLength", Long.toString(index.getCollectionLength()));
        featureParameters.add("documentCount", Long.toString(index.getDocumentCount()));
        featureFactory = new FeatureFactory(featureParameters);
    }

    public StructuredRetrieval(String filename, Parameters parameters)
            throws FileNotFoundException, IOException {
        this(new StructuredIndex(filename), parameters);
    }

    public StructuredIndex getIndex() {
        return index;
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

    public Node transformQuery(Node queryTree) throws Exception {
        List<Traversal> traversals = featureFactory.getTraversals(this);
        for (Traversal traversal : traversals) {
            queryTree = StructuredQuery.copy(traversal, queryTree);
        }
        return queryTree;
    }

    /**
     * Evaluates a query.
     *
     * @param queryTree A query tree that has been already transformed with StructuredRetrieval.transformQuery.
     * @param requested The number of documents to retrieve, at most.
     * @return
     * @throws java.lang.Exception
     */
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

    public String getDocumentName(int document) {
        return index.getDocumentName(document);
    }

    public void close() throws IOException {
        index.close();
    }
}
