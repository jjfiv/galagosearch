/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.index.merge;

import java.io.IOException;
import java.util.HashMap;
import org.galagosearch.core.types.DocumentMappingData;
import org.galagosearch.tupleflow.TypeReader;
import org.galagosearch.tupleflow.Utility;

/**
 * Maps a docid key (bytes) to a new docid key (bytes)
 *
 * @author sjh
 */
class DocumentMappingReader {

  HashMap<Integer, Integer> indexIncrements = new HashMap();

  DocumentMappingReader(TypeReader<DocumentMappingData> mappingDataStream) throws IOException {
    DocumentMappingData dat;
    while ((dat = mappingDataStream.read()) != null) {
      indexIncrements.put(dat.indexId, dat.docNumIncrement);
    }
  }

  public int map(int indexId, int docId) {
    return docId + indexIncrements.get(indexId);
  }

  byte[] map(int indexId, byte[] keyBytes) {
    return Utility.fromInt(map(indexId, Utility.toInt(keyBytes)));
  }
}
