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
public abstract class DocumentReader extends KeyValueReader {

  public DocumentReader(String filename) throws FileNotFoundException, IOException {
    super(filename);
  }

  public DocumentReader(GenericIndexReader r) {
    super(r);
  }

  public abstract Document getDocument(String key) throws IOException;

  public abstract class DocumentIterator extends KeyValueReader.Iterator {
    public DocumentIterator(GenericIndexReader reader) throws IOException {
      super(reader);
    }

    public abstract Document getDocument() throws IOException;
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
