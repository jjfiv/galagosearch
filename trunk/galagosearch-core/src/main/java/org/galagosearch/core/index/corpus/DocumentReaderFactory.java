/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.index.corpus;

import java.io.IOException;
import org.galagosearch.core.index.IndexReader;

/**
 *
 * @author sjh
 */
public class DocumentReaderFactory {

  public static DocumentReader instance(String path) throws IOException {
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
