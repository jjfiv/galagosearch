package org.galagosearch.core.retrieval.traversal;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.index.StructuredIndexPartReader;
import org.galagosearch.core.parse.TagTokenizer;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.core.scoring.TermSelectionValueModel;
import org.galagosearch.core.scoring.TermSelectionValueModel.Gram;
import org.galagosearch.core.util.TextPartAssigner;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 * We run the query as a combine on the way back up, and add in the
 * expansion terms. This is similar to the RelevanceModelTraversal.
 *
 * Little weird here - we transform an operator over a subtree into
 * low-level feature operators that act on count iterators.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"retrievalGroup", "index", "documentCount"})
public class BM25RelevanceFeedbackTraversal implements Traversal {

    Retrieval retrieval;
    Parameters queryParameters;
    Parameters availableParts;

    public BM25RelevanceFeedbackTraversal(Parameters parameters, Retrieval retrieval) throws IOException {
        this.retrieval = retrieval;
        this.queryParameters = parameters;
        this.availableParts = retrieval.getAvailableParts(parameters.get("retrievalGroup"));
    }

    public Node afterNode(Node original) throws Exception {
        if (original.getOperator().equals("bm25rf") == false) {
            return original;
        }

        // Kick off the inner query
        Parameters parameters = original.getParameters();
        int fbDocs = (int) parameters.get("fbDocs", 10);
        Node combineNode = new Node("combine", original.getInternalNodes());
        ArrayList<ScoredDocument> initialResults = new ArrayList<ScoredDocument>();

        // Only get as many as we need
        Parameters localParameters = queryParameters.clone();
        localParameters.set("requested", Integer.toString(fbDocs));

        retrieval.runAsynchronousQuery(combineNode, localParameters, initialResults);

        // while that's running, extract the feedback parameters
        int fbTerms = (int) parameters.get("fbTerms", 10);
        double fbOrigWt = parameters.get("fbOrigWt", 0.5);
        Parameters tsvParameters = queryParameters.clone();
        tsvParameters.set("fbDocs", Integer.toString(fbDocs));
        tsvParameters.add("part", availableParts.list("part"));
        TermSelectionValueModel tsvModel = new TermSelectionValueModel(tsvParameters);
        tsvModel.initialize();
        HashSet<String> stopwords = Utility.readStreamToStringSet(getClass().getResourceAsStream("/stopwords/inquery"));
        Set<String> queryTerms = StructuredQuery.findQueryTerms(combineNode, "extents");
        stopwords.addAll(queryTerms);

        // Now we wait
        retrieval.waitForAsynchronousQuery();
        Node newRoot = null;
        Node expansionNode = tsvModel.generateExpansionQuery(initialResults, fbTerms, stopwords);
        tsvModel.cleanup();

        if (fbOrigWt == 0.0) {
            newRoot = expansionNode;
        } else {
            Parameters expParams = new Parameters();
            expParams.set("1", Double.toString(fbOrigWt));
            expParams.set("2", Double.toString(1 - fbOrigWt));
            ArrayList<Node> newChildren = new ArrayList<Node>();
            newChildren.add(combineNode);
            newChildren.add(expansionNode);
            newRoot = new Node("combine", expParams, newChildren, 0);
        }
        return newRoot;
    }

    public void beforeNode(Node object) throws Exception {
        // do nothing
    }
}
