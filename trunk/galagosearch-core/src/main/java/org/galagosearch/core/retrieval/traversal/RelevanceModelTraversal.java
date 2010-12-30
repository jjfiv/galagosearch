package org.galagosearch.core.retrieval.traversal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.galagosearch.core.parse.TagTokenizer;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.core.util.TextPartAssigner;
import org.galagosearch.core.scoring.RelevanceModel;
import org.galagosearch.core.scoring.RelevanceModel.Gram;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 * The Relevance Model implemented as a traversal. Query should look like:
 *
 *  #rm:fbOrigWt=0.5:fbDocs=10:fbTerms=10( query )
 *
 * The outer node (the #rm operator) will be replaced with a #combine, and the 
 * query submitted to the retrieval supplied at construction. The parameters
 * will then be applied for constructing the expansion.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"retrievalGroup", "index"})
public class RelevanceModelTraversal implements Traversal {
    Retrieval retrieval;
    Parameters queryParameters;
    Parameters availableParts;

  public RelevanceModelTraversal(Parameters parameters, Retrieval retrieval) throws IOException {
      this.retrieval = retrieval;
      this.queryParameters = parameters;
      this.availableParts = retrieval.getAvailiableParts(parameters.get("retrievalGroup"));
  }

  public Node afterNode(Node newNode) throws Exception {
    if (newNode.getOperator().equals("rm") == false) return newNode;

    // Kick off the inner query
    Parameters parameters = newNode.getParameters();
    int fbDocs = (int) parameters.get("fbDocs", 10);
    Node combineNode = new Node("combine", newNode.getInternalNodes());
    ArrayList<ScoredDocument> initialResults = new ArrayList<ScoredDocument>();

    // Only get as many as we need
    Parameters localParameters = queryParameters.clone();
    localParameters.set("requested", Integer.toString(fbDocs));

    retrieval.runAsynchronousQuery(combineNode, localParameters, initialResults);

    RelevanceModel rModel = new RelevanceModel(queryParameters);
    rModel.initialize();
    double fbOrigWt = parameters.get("fbOrigWt", 0.5);
    int fbTerms = (int) parameters.get("fbTerms", 10);
    // Now we wait
    retrieval.waitForAsynchronousQuery();

    ArrayList<Gram> scored = rModel.generateGrams(initialResults);
    rModel.cleanup();
    Node newRoot = null;

    // Time to construct the modified query - start with the expansion since we always have it
    // make sure we filter stopwords
    HashSet<String> stopwords = Utility.readStreamToStringSet(getClass().getResourceAsStream("/stopwords/inquery"));
    Set<String> queryTerms = StructuredQuery.findQueryTerms(combineNode, "extents");
    stopwords.addAll(queryTerms);
    Parameters expParams = new Parameters();
    ArrayList<Node> newChildren = new ArrayList<Node>();
    int expanded = 0;
    for (int i=0; i < scored.size() && expanded < fbTerms; i++) {
      Gram g = scored.get(i);
      if (stopwords.contains(g.term)) continue;
      Node inner = TextPartAssigner.assignPart(new Node("text", g.term), availableParts);
      ArrayList<Node> innerChild = new ArrayList<Node>();
      innerChild.add(inner);
      newChildren.add(new Node("feature", "dirichlet", innerChild));
      expParams.set(Integer.toString(expanded), Double.toString(g.score));
      expanded++;
    }
    Node expansionNode = new Node("combine", expParams, newChildren, 0);

    if (fbOrigWt == 0.0) {
      expansionNode = newRoot;
    } else {
      expParams = new Parameters();
      expParams.set("1", Double.toString(fbOrigWt));
      expParams.set("2", Double.toString(1-fbOrigWt));
      newChildren = new ArrayList<Node>();
      newChildren.add(combineNode);
      newChildren.add(expansionNode);
      newRoot = new Node("combine", expParams, newChildren, 0);
    }
    return newRoot;
  }

  public void beforeNode(Node object) throws Exception {
  }
}
