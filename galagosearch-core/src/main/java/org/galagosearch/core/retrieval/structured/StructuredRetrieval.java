// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.structured;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.traversal.AddCombineTraversal;
import org.galagosearch.core.retrieval.traversal.ImplicitFeatureCastTraversal;
import org.galagosearch.core.retrieval.traversal.TextFieldRewriteTraversal;
import org.galagosearch.core.retrieval.traversal.WeightConversionTraversal;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class StructuredRetrieval extends Retrieval {
    StructuredIndex index;
    FeatureFactory featureFactory;

    public StructuredRetrieval(StructuredIndex index) {
        this.index = index;
        Parameters featureParameters = new Parameters();
        featureParameters.add("collectionLength", Long.toString(index.getCollectionLength()));
        featureParameters.add("documentCount", Long.toString(index.getDocumentCount()));
        featureFactory = new FeatureFactory(featureParameters);
    }

    public StructuredRetrieval(String filename) throws FileNotFoundException, IOException {
        this(new StructuredIndex(filename));
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
        queryTree = StructuredQuery.copy(new AddCombineTraversal(), queryTree);
        queryTree = StructuredQuery.copy(new TextFieldRewriteTraversal(index), queryTree);
        queryTree = StructuredQuery.copy(new WeightConversionTraversal(), queryTree);
        queryTree = StructuredQuery.copy(new ImplicitFeatureCastTraversal(this), queryTree);
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
