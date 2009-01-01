// BSD License (http://www.galagosearch.org/license)


package org.galagosearch.core.retrieval.query;

import java.util.ArrayList;

/**
 * 
 * @author trevor
 */
public interface Traversal {
    public void beforeNode(Node object) throws Exception;
    public Node afterNode(Node object, ArrayList<Node> children) throws Exception;
}
    
