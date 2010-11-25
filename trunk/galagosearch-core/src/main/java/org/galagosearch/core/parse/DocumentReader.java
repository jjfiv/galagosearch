// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.IOException;

/**
 * Interface that allows different corpus formats
 * See CorpusReader/DocumentIndexReader
 *
 * @author sjh
 */
public interface DocumentReader {
  public void close() throws IOException ;
  public DocumentIterator getIterator() throws IOException ;
  public Document getDocument(String key) throws IOException ;

  public interface DocumentIterator {
    public void skipTo(byte[] key) throws IOException;
    public String getKey();
    public boolean isDone();
    public Document getDocument() throws IOException;
    public boolean nextDocument() throws IOException;

  }
}
