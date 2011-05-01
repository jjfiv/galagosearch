/*
 * BSD License (http://www.galagosearch.org/license)

 */

package org.galagosearch.core.retrieval.structured;

import org.galagosearch.tupleflow.Parameters;

/**
 * Sole purpose of this class is to generate contexts for retrieval
 *
 * @author irmarc
 */
public class ContextFactory {

  // Can't instantiate it
  private ContextFactory() {}

  public static DocumentContext createContext(Parameters p) {
    if (p.get("topdocs", false)) {
      return new TopDocsContext();
    } else {
      return new DocumentContext();
    }
  }
}
