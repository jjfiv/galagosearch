/*
 * BSD License (http://www.galagosearch.org/license)

 */

package org.galagosearch.core.retrieval.structured;

import java.util.ArrayList;
import java.util.HashMap;
import org.galagosearch.core.index.TopDocsReader.TopDocument;

/**
 * Extension to the DocumentContext to support passing around topdocs
 *
 * @author irmarc
 */
public class TopDocsContext extends DocumentContext {
  public HashMap<ScoreValueIterator, ArrayList<TopDocument>> topdocs;
  public ArrayList<TopDocument> hold;

  public TopDocsContext() {
    super();
    topdocs = new HashMap<ScoreValueIterator, ArrayList<TopDocument>>();
  }
}
