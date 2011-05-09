// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.traversal;

import java.io.IOException;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.structured.CountIterator;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.core.retrieval.structured.ScoringFunctionIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 * When performing distributed retrieval it is necessary to collect
 * correct collection statistics.
 *
 * @author sjh
 */
@RequiredStatistics(statistics = {"retrievalGroup"})
public class DetermineCollectionProbabilities implements Traversal {
  Retrieval retrieval;
  String retrievalGroup;
  Parameters stats;

  public DetermineCollectionProbabilities(Parameters parameters, Retrieval retrieval) throws IOException {
    this.retrieval = retrieval;
    this.retrievalGroup = parameters.get("retrievalGroup");
    this.stats = retrieval.getRetrievalStatistics(retrievalGroup);
  }

  // Count iterator check
  private boolean isCountNode(Node node) throws Exception {
    NodeType nodeType = retrieval.getNodeType(node, retrievalGroup);
    if (nodeType == null) {
      return false;
    }
    Class outputClass = nodeType.getIteratorClass();
    return CountIterator.class.isAssignableFrom(outputClass);
  }

  // Smoothing functions (takes count iterators, emits a score)
  private boolean isScoringFnNode(Node node) throws Exception {
    NodeType nodeType = retrieval.getNodeType(node, retrievalGroup);
    if (nodeType == null) {
      return false;
    }
    Class outputClass = nodeType.getIteratorClass();
    return ScoringFunctionIterator.class.isAssignableFrom(outputClass);
  }

  private double getCollectionProbability(long collectionCount) {
    long collectionLength = stats.get("collectionLength", (long) 0);

    if (collectionCount > 0) {
      return ((double) collectionCount / (double) collectionLength);
    } else {
      return (0.5 / (double) collectionLength);
    }
  }

  public void beforeNode(Node node) throws Exception {
  }

  public Node afterNode(Node node) throws Exception {
    Parameters newParameters = node.getParameters().clone();

    NodeType nodeType = retrieval.getNodeType(node, retrievalGroup);
    if ((nodeType != null) && (isScoringFnNode(node))) {
      Node child = node.getInternalNodes().get(0);

      if (isCountNode(child)) {
        // Use xcount to count the background probabilities
        // TODO: make retrievalGroup a parameters for xcount
        node.getParameters().add("retrievalGroup", retrievalGroup);
        double collectionProb = getCollectionProbability(retrieval.xCount(node.toString()));

        // add the collection probability to the scoring function node
        newParameters.add("collectionCount", Double.toString(collectionProb));
        return new Node(node.getOperator(), newParameters, node.getInternalNodes(), node.getPosition());

      } else {
        // Something is wrong - a ScoringFunctionIterator should have a single count iterator child
        return node;
      }
    } else {
      return node;
    }
  }
}
