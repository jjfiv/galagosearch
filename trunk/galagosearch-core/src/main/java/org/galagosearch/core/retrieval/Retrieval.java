// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval;

import java.io.IOException;
import java.util.List;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.tupleflow.Parameters;

/**
 * <p>This is a base class for all kinds of retrieval classes.  Historically this was
 * used to support binned indexes in addition to structured indexes.</p>
 *
 * @author trevor, irmarc
 */
public interface Retrieval extends Runnable {

    public void close() throws IOException;

    // should return collections statistics (collection length + documentCount) for each part
    // retrievalGroup controls the set of retrievals that should be used -- MultiRetrieval only
    public Parameters getRetrievalStatistics() throws IOException;
    public Parameters getRetrievalStatistics(String retrievalGroup) throws IOException;

    // should return availiable parts (postings + extents + ...) and nodeTypes (count + extents)
    // retrievalGroup controls the set of retrievals that should be used -- MultiRetrieval only

    public Parameters getAvailableParts(String retrievalGroup) throws IOException;

    public NodeType getNodeType(Node node, String retrievalGroup) throws Exception;

    public Node transformBooleanQuery(Node root, String retrievalGroup) throws Exception;

    public Node transformCountQuery(Node root, String retrievalGroup) throws Exception;

    public Node transformRankedQuery(Node root, String retrievalGroup) throws Exception;

    public long xCount(String nodeString) throws Exception;

    public long xCount(Node root) throws Exception;

    public long docCount(String nodeString) throws Exception;

    public long docCount(Node root) throws Exception;

    public ScoredDocument[] runBooleanQuery(Node root, Parameters parameters) throws Exception;

    public ScoredDocument[] runRankedQuery(Node root, Parameters parameters) throws Exception;

    // These are to allow for asynchronous execution
    public void runAsynchronousQuery(Node root, Parameters parameters, List<ScoredDocument> queryResults, List<String> errors) throws Exception;

    public void waitForAsynchronousQuery() throws InterruptedException;
}
