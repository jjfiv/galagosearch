// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.parse;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.galagosearch.core.index.IndexReader;

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

  public abstract interface DocumentIterator {

    public void skipTo(byte[] key) throws IOException;

    public String getKey();

    public boolean isDone();

    public Document getDocument() throws IOException;

    public boolean nextDocument() throws IOException;
  }

  public static DocumentReader getInstance(String path) throws IOException {
    if(isCorpus(path)){
      return new CorpusReader(path);
    } else if (IndexReader.isIndexFile(path)){
      return new DocumentIndexReader(path);
    } else {
      throw new IOException( "Path is not a known corpus format: " + path );
    }
  }

  /*
   * Checks that there is an index.corpus file
   * and at least one .cds or .cds.z file
   * in the provided directory or in the parent directory of a file
   *
   */
  public static boolean isCorpus(String fileName) {
    File f = new File(fileName);

    assert f.exists() : "Corpus file does not exist at path: " + f.getAbsolutePath();

    if (!f.isDirectory()) {
      f = f.getParentFile();
    }

    boolean index = new File(f.getAbsolutePath() + File.separator + "index.corpus").exists();
    boolean cds = false;
    for (File sibling : f.listFiles()) {
      if (sibling.getName().endsWith(".cds") || sibling.getName().endsWith(".cds.z")) {
        cds = true;
      }
      if (cds && index) {
        return true;
      }
    }
    System.err.println( "fileName -- " + index + " -- " + cds );

    return false;
  }
}
