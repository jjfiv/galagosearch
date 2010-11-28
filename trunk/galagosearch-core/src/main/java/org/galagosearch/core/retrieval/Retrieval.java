// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval;

import java.io.IOException;
import java.util.List;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.structured.StructuredRetrieval;
import org.galagosearch.core.retrieval.structured.StructuredRetrievalProxy;
import org.galagosearch.tupleflow.Parameters;

/**
 * <p>This is a base class for all kinds of retrieval classes.  Historically this was
 * used to support binned indexes in addition to structured indexes.</p>
 *
 * @author trevor, irmarc
 */
public abstract class Retrieval implements Runnable {
    public abstract String getDocumentName(int document) throws IOException;
    /**
     * Transforms the query into a more complete representation that can
     * be directly executed.
     */
    public abstract Node transformQuery(Node query) throws Exception;
    public abstract ScoredDocument[] runQuery(Node query, int requested) throws Exception;
    public abstract void runAsynchronousQuery(Node query, int requested, List<ScoredDocument> scored, int idx) throws Exception;
    public abstract void close() throws IOException;
    
    // These are to allow for asynchronous execution
    public abstract void run();
    public abstract void join() throws InterruptedException;

	
	// Hacks I added to make distributed retrieval work -- irmarc
    public ScoredDocument[] runQuery(String query, int requested) throws Exception {
		throw new Exception ("Not appropriate in this context.");
    }
    
    public void runAsynchronousQuery(String query, int requested, List<ScoredDocument> scored, int idx) throws Exception {
		throw new Exception ("Not appropriate in this context.");
    }

    // defaults to true -- proxyRetrievals should return false;
    public boolean isLocal() { return true; }
    
    static public Retrieval instance(String indexPath, Parameters parameters) throws IOException {
        // return new StructuredRetrieval(indexPath, parameters);
		String queryType = parameters.get("queryType", "complex");
		
		if (queryType.equals("simple")) throw new IllegalArgumentException("Not supporting simple queries");
		
		// May need something smarter here in the future
		if (indexPath.startsWith("http://")) {
			return new StructuredRetrievalProxy(indexPath, parameters);
		} else {
			return new StructuredRetrieval(indexPath, parameters);
		}
    }
}
