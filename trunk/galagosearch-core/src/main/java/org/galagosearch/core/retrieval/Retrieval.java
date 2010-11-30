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

  public abstract void close() throws IOException;

  // should return collections statistics (collection length + documentCount + availiable parts)
  public abstract Parameters getRetrievalStatistics() throws Exception;

  /**
   * Returns the count of the expression provided to the method. Therefore the
   * string passed should represent a node type that will produce a CountIterator.
   *
   * @param nodeString
   * @return The count of the provided expression
   * @throws Exception
   */
  public abstract long xcount(String nodeString) throws Exception;

  public abstract ScoredDocument[] runQuery(String query, Parameters parameters) throws Exception;

  // These are to allow for asynchronous execution
  public abstract void runAsynchronousQuery(String query, Parameters parameters, List<ScoredDocument> queryResults) throws Exception;
  public abstract void waitForAsynchronousQuery() throws InterruptedException;

  static public Retrieval instance(String indexPath, Parameters parameters) throws IOException {
    // May need something smarter here in the future
    if (indexPath.startsWith("http://")) {
      return new StructuredRetrievalProxy(indexPath, parameters);
    } else {
      return new StructuredRetrieval(indexPath, parameters);
    }
  }
}
