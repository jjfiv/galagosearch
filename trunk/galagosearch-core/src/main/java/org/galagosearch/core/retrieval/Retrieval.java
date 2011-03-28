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
import org.galagosearch.core.retrieval.structured.StructuredRetrievalProxy;
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
public abstract class Retrieval implements Runnable {

    public abstract void close() throws IOException;

    // should return collections statistics (collection length + documentCount)
    // retrievalGroup controls the set of retrievals that should be used -- MultiRetrieval only
    public abstract Parameters getRetrievalStatistics(String retrievalGroup) throws IOException;
    // should return availiable parts (postings + extents + ...) and nodeTypes (count + extents)
    // retrievalGroup controls the set of retrievals that should be used -- MultiRetrieval only

    public abstract Parameters getAvailableParts(String retrievalGroup) throws IOException;

    public abstract NodeType getNodeType(Node node, String retrievalGroup) throws Exception;
    // provides a way to instantiate a node, given a retrieval object that has an index and
    // featurefactory

    public abstract StructuredIterator createIterator(Node node) throws Exception;

    /**
     * Returns the count of the expression provided to the method. Therefore the
     * string passed should represent a node type that will produce a CountIterator.
     *
     * @param nodeString
     * @return The count of the provided expression
     * @throws Exception
     */
    public abstract long xCount(String nodeString) throws Exception;

    public abstract long xCount(Node root) throws Exception;

    public abstract long docCount(String nodeString) throws Exception;

    public abstract long docCount(Node root) throws Exception;

    public abstract ScoredDocument[] runQuery(Node root, Parameters parameters) throws Exception;

    public abstract Node transformQuery(Node root, String retrievalGroup) throws Exception;

    // These are to allow for asynchronous execution
    public abstract void runAsynchronousQuery(Node root, Parameters parameters, List<ScoredDocument> queryResults) throws Exception;

    public abstract void waitForAsynchronousQuery() throws InterruptedException;

    // This function allows parameter sweep queries to be run
    public abstract ScoredDocument[] runParameterSweep(Node root, Parameters parameters) throws Exception;

    /* get retrieval object
     * cases:
     *  1 index path - local
     *  1 index path - proxy
     *  many index paths - multi - local
     *  many index paths - multi - drmaa
     */
    static public Retrieval instance(String path, Parameters parameters) throws IOException {
        if (path.startsWith("http://")) {
            return new StructuredRetrievalProxy(path, parameters);
        } else {
            // check for drmaa
            return new StructuredRetrieval(path, parameters);
        }
    }

    static public Retrieval instance(Parameters parameters) throws IOException, Exception {
        List<Value> indexes = parameters.list("index");

        String path, id;

        // first check if there is only one index provided.
        if (indexes.size() == 1) {
            Value value = indexes.get(0);
            if (value.containsKey("path")) {
                path = value.get("path").toString();
            } else {
                path = value.toString();
            }
            return instance(path, parameters);
        }

        // otherwise we have a multi-index
        HashMap<String, ArrayList<Retrieval>> retrievals = new HashMap();
        for (Value value : indexes) {
            id = "all";
            if (value.containsKey("path")) {
                path = value.get("path").toString();
                if (value.containsKey("id")) {
                    id = value.get("id").toString();
                }
            } else {
                path = value.toString();
            }
            if (!retrievals.containsKey(id)) {
                retrievals.put(id, new ArrayList<Retrieval>());
            }

            try {
                Retrieval r = instance(path, parameters);
                retrievals.get(id).add(r);
                if (!id.equals("all")) {
                    retrievals.get("all").add(r); // Always put it in default as well
                }
            } catch (Exception e) {
                System.err.println("Unable to load index (" + id + ") at path " + path + ": " + e.getMessage());
            }
        }

        return new MultiRetrieval(retrievals, parameters);
    }
}
