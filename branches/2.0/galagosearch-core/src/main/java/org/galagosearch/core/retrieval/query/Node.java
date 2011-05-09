// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.Parameters;

/**
 * <p>Node represents a single node in a query parse tree.</p>
 * 
 * <p>In Galago, queries are parsed into a tree of Nodes.  The query tree can then
 * be modified using StructuredQuery.copy, or analyzed by using StructuredQuery.walk.
 * Once the query is in the proper form, the query is converted into a tree of iterators
 * that can be evaluated.</p>
 * 
 * @author trevor
 */
public class Node {
    /// The query operator represented by this node, like "combine", "weight", "syn", etc.
    private String operator;

    /// Child nodes of the operator, e.g. in #combine(a b), 'a' and 'b' are internal nodes of #combine.
    private ArrayList<Node> internalNodes;

    /// The position in the text string where this operator starts.  Useful for parse error messages.
    private int position;

    /// Additional parameters for this operator; usually these are term statistics and smoothing parameters.
    private Parameters parameters;

    public Node() {
        internalNodes = new ArrayList<Node>();
        parameters = new Parameters();
    }

    public Node(String operator, ArrayList<Node> internalNodes) {
        this(operator, (String) null, internalNodes, 0);
    }

    public Node(String operator, ArrayList<Node> internalNodes, int position) {
        this(operator, (String) null, internalNodes, position);
    }

    public Node(String operator, String argument) {
        this(operator, argument, 0);
    }

    public Node(String operator, String argument, int position) {
        this(operator, argument, new ArrayList<Node>(), position);
    }

    public Node(String operator, String argument, ArrayList<Node> internalNodes) {
        this(operator, argument, internalNodes, 0);
    }

    public Node(String operator, String argument, ArrayList<Node> internalNodes, int position) {
        Parameters p = new Parameters();

        if (argument != null) {
            p.add("default", argument);
        }
        this.operator = operator;
        this.internalNodes = internalNodes;
        this.position = position;
        this.parameters = p;
    }

    public Node(String operator, Parameters parameters, ArrayList<Node> internalNodes, int position) {
        this.operator = operator;
        this.internalNodes = internalNodes;
        this.position = position;
        this.parameters = parameters;
    }

    public String getOperator() {
        return operator;
    }
    
    public String getDefaultParameter() {
        return parameters.get("default", (String)null);
    }
    
    public String getDefaultParameter(String key) {
        return parameters.get(key, getDefaultParameter());
    }

    public ArrayList<Node> getInternalNodes() {
        return internalNodes;
    }

    public int getPosition() {
        return position;
    }

    public Parameters getParameters() {
        return parameters;
    }
    
    public boolean needsToBeEscaped(String text) {
        return text.contains("@") || text.contains(",") ||
               text.contains(".") || text.contains(" ") ||
               text.contains("\t") || text.contains("\r") ||
               text.contains("\n");
    }
    
    public String escapeAsNecessary(String text) {
        if (!needsToBeEscaped(text)) {
            return text; 
        } else {
            String[] preferredDelimiters = { "/", "|", "#", "!", "%" };
            
            for (String delimiter : preferredDelimiters) {
                if (!text.contains(delimiter)) {
                    return "@" + delimiter + text + delimiter;
                }
            }
            
            // give up
            return text;
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append('#');
        builder.append(operator);

        if (parameters.containsKey("default")) {
            String value = parameters.get("default");
            builder.append(':');
            builder.append(escapeAsNecessary(value));
        }

        Map<String, List<Value>> parameterMap = parameters.value().map();

        if (parameterMap != null) {
            for (String key : parameterMap.keySet()) {
                if (key.equals("default")) {
                    continue;
                }
                String value = parameterMap.get(key).get(0).toString();
                
                builder.append(':');
                builder.append(escapeAsNecessary(key));
                builder.append('=');
                builder.append(escapeAsNecessary(value));
            }
        }

        if (internalNodes.size() == 0) {
            builder.append("()");
        } else {
            builder.append("( ");
            for (Node child : internalNodes) {
                builder.append(child.toString());
                builder.append(' ');
            }
            builder.append(")");
        }
        
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Node)) {
            return false;
        }
        if (o == this) {
            return true;
        }
        Node other = (Node) o;

        if ((operator == null) != (other.getOperator() == null)) {
            return false;
        }
        if (operator != null && !other.getOperator().equals(operator)) {
            return false;
        }
        if (internalNodes.size() != other.getInternalNodes().size()) {
            return false;
        }
        for (int i = 0; i < internalNodes.size(); i++) {
            if (!internalNodes.get(i).equals(other.getInternalNodes().get(i))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 67 * hash + (this.operator != null ? this.operator.hashCode() : 0);
        hash = 67 * hash + (this.internalNodes != null ? this.internalNodes.hashCode() : 0);
        return hash;
    }
}
