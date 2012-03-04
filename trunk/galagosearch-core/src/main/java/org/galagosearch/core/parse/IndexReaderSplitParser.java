// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.tupleflow.Utility;

/**
 * Reads Document data from an index file.  Typically you'd use this parser by
 * including UniversalParser in a TupleFlow Job.
 * 
 * @author trevor
 */
public class IndexReaderSplitParser implements DocumentStreamParser {
    DocumentIndexReader.Iterator iterator;
    DocumentSplit split;
    
    public IndexReaderSplitParser(DocumentSplit split) throws FileNotFoundException, IOException {
        DocumentIndexReader reader = new DocumentIndexReader(split.fileName);
        iterator = reader.getIterator();
        iterator.skipTo(split.startKey);
        this.split = split;
    }
    
    public Document nextDocument() throws IOException {
        if (iterator.isDone()) {
            return null;
        }
        
        String key = iterator.getKey();
        byte[] keyBytes = Utility.makeBytes(key);
        
        // Don't go past the end of the split.
        if (split.endKey.length > 0 && Utility.compare(keyBytes, split.endKey) >= 0) {
            return null;
        }

        Document document = iterator.getDocument();
        iterator.nextDocument();
        return document;
    }
}
