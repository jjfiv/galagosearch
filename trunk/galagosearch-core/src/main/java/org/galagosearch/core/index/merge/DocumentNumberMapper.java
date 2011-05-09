/*
 *  BSD License (http://www.galagosearch.org/license)
 */

package org.galagosearch.core.index.merge;

import java.io.File;
import java.io.IOException;
import org.galagosearch.core.index.DocumentNameReader;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.types.DocumentMappingData;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass( className="org.galagosearch.core.types.DocumentSplit", order = {"+fileId"})
@OutputClass( className="org.galagosearch.core.types.DocumentMappingData", order = {"+indexId"})
public class DocumentNumberMapper extends StandardStep<DocumentSplit, DocumentMappingData>{
  
  int nextIndexStartNumber = 0;

  public void process(DocumentSplit index) throws IOException {
    processor.process( new DocumentMappingData(index.fileId, nextIndexStartNumber) );

    DocumentNameReader namesReader = (DocumentNameReader) StructuredIndex.openIndexPart(index.fileName + File.separator + "names");
    DocumentNameReader.KeyIterator iterator = namesReader.getIterator();
    int lastDocId = iterator.getCurrentIdentifier();
    while(iterator.nextKey()){
      lastDocId = iterator.getCurrentIdentifier();
    }
    nextIndexStartNumber += lastDocId + 1;
  }

}
