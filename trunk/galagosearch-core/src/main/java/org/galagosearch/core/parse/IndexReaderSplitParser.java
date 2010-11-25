// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.galagosearch.core.parse.DocumentReader.DocumentIterator;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.tupleflow.Utility;

/**
 * Reads Document data from an index file.  Typically you'd use this parser by
 * including UniversalParser in a TupleFlow Job.
 * 
 * @author trevor, sjh
 */
public class IndexReaderSplitParser implements DocumentStreamParser {
  DocumentReader reader;
  DocumentIterator iterator;
  DocumentSplit split;

  public IndexReaderSplitParser(DocumentSplit split) throws FileNotFoundException, IOException {
    if(CorpusReader.isCorpus(split.fileName)){
      reader = new CorpusReader(split.fileName);
    } else {
      reader = new DocumentIndexReader(split.fileName);
    }
    iterator = reader.getIterator();
    iterator.skipTo(split.startKey);
    this.split = split;
  }

  public Document nextDocument() throws IOException {
    if (iterator.isDone()) {
      reader.close();
      return null;
    }

    String key = iterator.getKey();
    byte[] keyBytes = Utility.makeBytes(key);

    // Don't go past the end of the split.
    if (split.endKey.length > 0 && Utility.compare(keyBytes, split.endKey) >= 0) {
      reader.close();
      return null;
    }

    Document document = iterator.getDocument();
    iterator.nextDocument();
    return document;
  }

}
