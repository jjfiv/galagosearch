// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.corpus;

import java.io.IOException;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.core.parse.Document;

/**
 * Interface that allows different corpus formats
 * See CorpusReader/DocumentIndexReader
 *
 * @author sjh
 */
public abstract class DocumentReader {

  public abstract void close() throws IOException;

  public abstract DocumentIterator getIterator() throws IOException;

  public abstract Document getDocument(String key) throws IOException;

  public interface DocumentIterator extends Comparable<DocumentIterator> {

    public void skipTo(byte[] key) throws IOException;

    public String getKey();

    public byte[] getKeyBytes();

    public boolean isDone();

    public Document getDocument() throws IOException;

    public boolean nextDocument() throws IOException;
  }

  public static DocumentReader getInstance(String path) throws IOException {
    if (isCorpus(path)) {
      return new CorpusReader(path);
    } else if (IndexReader.isIndexFile(path)) {
      return new DocumentIndexReader(path);
    } else {
      throw new IOException("Path is not a known corpus format: " + path);
    }
  }

  /*
   * Checks if it is a corpus folder structure
   *  - file structure can be checked using isIndexFile(path)
   */
  public static boolean isCorpus(String fileName) throws IOException {
    return SplitIndexReader.isParallelIndex(fileName);
  }
}
