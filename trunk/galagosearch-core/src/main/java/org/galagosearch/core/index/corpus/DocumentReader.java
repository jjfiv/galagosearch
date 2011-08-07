// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.corpus;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.galagosearch.core.index.GenericIndexReader;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.core.index.KeyIterator;
import org.galagosearch.core.index.KeyValueReader;
import org.galagosearch.core.index.StructuredIndexPartReader;
import org.galagosearch.core.parse.Document;

/**
 * Interface that allows different corpus formats
 * See CorpusReader/DocumentIndexReader
 *
 * @author sjh
 */
public interface DocumentReader extends StructuredIndexPartReader {

  public abstract Document getDocument(String key) throws IOException;

  public interface DocumentIterator extends KeyIterator {

    public abstract Document getDocument() throws IOException;
  }
}
