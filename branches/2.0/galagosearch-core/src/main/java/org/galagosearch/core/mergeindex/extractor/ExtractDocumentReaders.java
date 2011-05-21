// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.mergeindex.extractor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.index.corpus.DocumentReader;
import org.galagosearch.core.index.corpus.DocumentReader;
import org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator;
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
@InputClass(className = "org.galagosearch.core.types.DocumentSplit")
@OutputClass(className = "org.galagosearch.core.index.corpus.DocumentReader")
public class ExtractDocumentReaders extends StandardStep<DocumentSplit, DocumentReader> {
  ArrayList<StructuredIndex> s = new ArrayList();

  public void process(DocumentSplit file) throws IOException {
    DocumentReader docReader = DocumentReader.getInstance( file.fileName + File.separator + "corpus" );
    processor.process( docReader );
  }
}
