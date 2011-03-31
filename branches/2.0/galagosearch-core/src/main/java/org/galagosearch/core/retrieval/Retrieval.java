// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.StructuredIterator;
import org.galagosearch.core.retrieval.structured.StructuredRetrieval;
import org.galagosearch.core.retrieval.structured.RetiredProxy;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Parameters.Value;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.Session;
import org.ggf.drmaa.SessionFactory;

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

    /**
     * Returns the count of the expression provided to the method. Therefore the
     * string passed should represent a node type that will produce a CountIterator.
     *
     * @param nodeString
     * @return The count of the provided expression
     * @throws Exception
     */
    public long xCount(String nodeString) throws Exception;

    public long xCount(Node root) throws Exception;

    public long docCount(String nodeString) throws Exception;

    public long docCount(Node root) throws Exception;

    public ScoredDocument[] runBooleanQuery(Node root, Parameters parameters) throws Exception;

    public Node transformBooleanQuery(Node root, String retrievalGroup) throws Exception;

    public ScoredDocument[] runRankedQuery(Node root, Parameters parameters) throws Exception;

    public Node transformRankedQuery(Node root, String retrievalGroup) throws Exception;

    // These are to allow for asynchronous execution
    public void runAsynchronousQuery(Node root, Parameters parameters, List<ScoredDocument> queryResults) throws Exception;

    public void waitForAsynchronousQuery() throws InterruptedException;

    // This function allows parameter sweep queries to be run
    public ScoredDocument[] runParameterSweep(Node root, Parameters parameters) throws Exception;
}
