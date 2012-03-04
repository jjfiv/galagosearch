// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval;

import java.io.IOException;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.structured.StructuredRetrieval;
import org.galagosearch.tupleflow.Parameters;

/**
 * <p>This is a base class for all kinds of retrieval classes.  Historically this was
 * used to support binned indexes in addition to structured indexes.</p>
 *
 * @author trevor
 */
public abstract class Retrieval {
    public abstract String getDocumentName(int document) throws IOException;
    /**
     * Transforms the query into a more complete representation that can
     * be directly executed.
     */
    public abstract Node transformQuery(Node query) throws Exception;
    public abstract ScoredDocument[] runQuery(Node query, int requested) throws Exception;
    public abstract void close() throws IOException;
    
    static public Retrieval instance(String indexPath, Parameters parameters) throws IOException {
        return new StructuredRetrieval(indexPath, parameters);
    }
}
