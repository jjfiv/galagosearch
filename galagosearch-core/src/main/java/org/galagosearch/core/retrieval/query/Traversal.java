// BSD License (http://www.galagosearch.org/license)


package org.galagosearch.core.retrieval.query;

import java.util.ArrayList;

/**
 * 
 * @author trevor
 */
public interface Traversal {
    public Node afterNode(Node newNode) throws Exception;
    public void beforeNode(Node object) throws Exception;
}
    
