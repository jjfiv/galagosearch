// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.traversal;

import java.util.ArrayList;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.structured.CountIterator;
import org.galagosearch.core.retrieval.structured.ScoreIterator;
import org.galagosearch.core.retrieval.structured.ScoringFunctionIterator;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.structured.DocumentOrderedFeatureFactory;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.core.retrieval.structured.StructuredRetrieval;
import org.galagosearch.tupleflow.Parameters;

/**
 * For many kinds of queries, it may be preferable to not have to type
 * an explicit #feature operator around a count or extents term.  For example,
 * we want #combine(#feature:dirichlet(#counts:dog())) to be the same as
 * #combine(dog).  This transformation automatically adds the #feature:dirichlet
 * operator.
 * 
 * (12/21/2010, irmarc): Modified to annotate topdocs feature nodes, as well as generate them
 *                        if specified by construction parameters.
 *
 * @author trevor, irmarc
 */
@RequiredStatistics(statistics = {"retrievalGroup", "index","scorer"})
public class ImplicitFeatureCastTraversal implements Traversal {
    Retrieval retrieval;
    String retrievalGroup;
    Parameters parameters;

    public ImplicitFeatureCastTraversal(Parameters parameters, Retrieval retrieval) {
        this.retrieval = retrieval;
        this.retrievalGroup = parameters.get("retrievalGroup");
	this.parameters = parameters;
    }
    
    Node createSmoothingNode(Node child) {
        ArrayList<Node> data = new ArrayList<Node>();
        data.add(child);
	String scorerType = parameters.get("scorer", "dirichlet");
        return new Node("feature", scorerType, data, child.getPosition());
    }
    
    public boolean isCountNode(Node node) throws Exception {
        NodeType nodeType = retrieval.getNodeType(node, retrievalGroup);
        if (nodeType == null) return false;
        Class outputClass = nodeType.getIteratorClass();
        return CountIterator.class.isAssignableFrom(outputClass);
    }

    public boolean isScoringFunctionNode(Node node) throws Exception {
        NodeType nodeType = retrieval.getNodeType(node, retrievalGroup);
        if (nodeType == null) return false;
        Class outputClass = nodeType.getIteratorClass();
        return ScoringFunctionIterator.class.isAssignableFrom(outputClass);
    }

    public void beforeNode(Node node) throws Exception {
    }

    
    public Node afterNode(Node node) throws Exception {
	// Determine if we need to add a scoring node
	Node scored = addScorers(node);

	// Add a topdocs wrapper node if necessary
	Node topped = addTopDocs(scored);
	return topped;
    }

    public Node addTopDocs(Node node) throws Exception {
	Node workingNode = node;

	// If this isn't a topdocs node, are we adding one?
	if (!workingNode.getParameters().get("default","none").equals("topdocs")) {
	    // Only wrap ScoringFunctionIterators
	    if (!isScoringFunctionNode(node)) return node;
	    
	    // if not adding topdocs, short-circuit out
	    if (!parameters.get("topdocs", false)) return node;
	    ArrayList<Node> child = new ArrayList<Node>();
	    child.add(workingNode);
	    workingNode = new Node("feature", "topdocs", child, workingNode.getPosition());
	}

	// At this point, we only annotate the node if it's a topdocs node
	if (workingNode.getOperator().equals("feature") &&
	    workingNode.getParameters().get("default","none").equals("topdocs")) {
	    // First (and only) child should be a scoring function iterator node
	    Node child = node.getInternalNodes().get(0);
	    if (!isScoringFunctionNode(child)) return node;

	    // count node, with the information we need
	    Node grandchild = child.getInternalNodes().get(0);
	    Parameters descendantParameters = grandchild.getParameters();
	    Parameters workingParameters = workingNode.getParameters();
	    workingParameters.set("term", descendantParameters.get("default"));
	    workingParameters.set("loc", descendantParameters.get("part"));
	    workingParameters.set("index", parameters.get("index"));
	}

	return workingNode;
    }

    public Node addScorers(Node node) throws Exception {
        ArrayList<Node> newChildren = new ArrayList<Node>();
        
        NodeType nodeType = retrieval.getNodeType(node, retrievalGroup);
        if (nodeType == null) return node;

        ArrayList<Node> children = node.getInternalNodes();
        // Given that we're going to pass children.size() + 1 parameters to
        // this constructor, what types should those parameters have?
        Class[] types = nodeType.getParameterTypes(children.size() + 1);
        if (types == null) return node;

        for (int i = 1; i < types.length; ++i) {
            Node child = children.get(i-1);
            // If the parent will expect a ScoreIterator at this position, but
            // we've got a CountIterator here, we'll perform a conversion step.
            if (ScoreIterator.class.isAssignableFrom(types[i]) &&
                isCountNode(children.get(i-1))) {
                Node feature = createSmoothingNode(child);
                newChildren.add(feature);
            } else {
                newChildren.add(child);
            }
        }

        return new Node(node.getOperator(), node.getParameters(),
                        newChildren, node.getPosition());
    }
}

