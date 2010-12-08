// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.parse;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.zip.GZIPInputStream;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.tupleflow.DataStream;
import org.galagosearch.tupleflow.StreamCreator;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.VByteInput;

/**
 *
 * Reader for corpus files
 *
 * @author sjh
 */
public class CorpusReader implements DocumentReader {

  String corpusFolder;
  String corpusIndex;
  IndexReader indexReader;

  public CorpusReader(String fileName) throws FileNotFoundException, IOException {
    if (new File(fileName).isFile()) {
      corpusFolder = new File(fileName).getParent();
      corpusIndex = fileName;

    } else { // isDirectory.
      corpusFolder = fileName;
      corpusIndex = fileName + File.separator + "index.corpus";
    }

    indexReader = new IndexReader(corpusIndex);
  }

  public CorpusReader() {
    // do nothing -- shouldn't really be used by anyone ever
    throw new UnsupportedOperationException("NO!");
  }

  public void close() throws IOException {
    indexReader.close();
  }

  public DocumentReader.DocumentIterator getIterator() throws IOException {
    return new Iterator(indexReader.getIterator());
  }

  public Document getDocument(String key) throws IOException {
    IndexReader.Iterator iterator = indexReader.getIterator(Utility.fromString(key));
    if (iterator == null) {
      return null;
    }
    return new Iterator(iterator).getDocument();
  }

  public class Iterator implements DocumentReader.DocumentIterator {

    IndexReader.Iterator iterator;

    Iterator(IndexReader.Iterator iterator) throws IOException {
      this.iterator = iterator;
    }

    public void skipTo(byte[] key) throws IOException {
      iterator.skipTo(key);
    }

    public String getKey() {
      return Utility.toString(iterator.getKey());
    }

    public boolean isDone() {
      return iterator.isDone();
    }

    public Document getDocument() throws IOException {
      String key = Utility.toString(iterator.getKey());
      DataStream stream = iterator.getValueStream();
      return decodeDocument(key, stream);
    }

    public boolean nextDocument() throws IOException {
      return iterator.nextKey();
    }

    private Document decodeDocument(String key, DataStream stream) throws IOException {
      VByteInput input = new VByteInput(stream);
      String fileName = corpusFolder + File.separator + input.readString();
      long offset = input.readLong();

      InputStream docInput;
      docInput = StreamCreator.realInputStream(fileName);

      docInput.skip(offset);
      ObjectInputStream docInput2;
      if (fileName.endsWith(".z")) {
        docInput2 = new ObjectInputStream(new GZIPInputStream(docInput));
      } else {
        docInput2 = new ObjectInputStream(docInput);
      }

      Document document = null;
      try {
        document = (Document) docInput2.readObject();
      } catch (ClassNotFoundException ex) {
        throw new IOException("Expected to find a serialized document here, " + "but found something else instead.", ex);
      }

      docInput.close();
      docInput2.close();

      return document;
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

    assert f.exists(): "Corpus file does not exist: " + f.getAbsolutePath();

    if( ! f.isDirectory() ){
      f = f.getParentFile();
    }

    boolean index = false;
    boolean cds = false;
    for (File sibling : f.listFiles()) {
      if (sibling.getName().endsWith(".cds") || sibling.getName().endsWith(".cds.z")) {
        cds = true;
        continue;
      }
      if (sibling.getName().endsWith("index.corpus")) {
        index = true;
      }
    }
    if (cds && index)
      return true;
    return false;
  }
}
