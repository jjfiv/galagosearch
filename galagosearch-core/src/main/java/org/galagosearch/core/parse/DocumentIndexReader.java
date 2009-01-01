// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.tupleflow.DataStream;
import org.galagosearch.tupleflow.VByteInput;

/**
 *
 * @author trevor
 */
public class DocumentIndexReader {
    IndexReader reader;

    public DocumentIndexReader(String fileName) throws FileNotFoundException, IOException {
        reader = new IndexReader(fileName);
    }

    public DocumentIndexReader(IndexReader reader) {
        this.reader = reader;
    }

    public void close() throws IOException {
        reader.close();
    }

    public Iterator getIterator() throws IOException {
        return new Iterator(reader.getIterator());
    }

    public Document getDocument(String key) throws IOException {
        IndexReader.Iterator iterator = reader.getIterator(key);
        if (iterator == null) return null;
        return new Iterator(iterator).getDocument();
    }

    public class Iterator {
        IndexReader.Iterator iterator;

        Iterator(IndexReader.Iterator iterator) throws IOException {
            this.iterator = iterator;
        }

        public void skipTo(byte[] key) throws IOException {
            iterator.skipTo(key);
        }

        public String getKey() {
            return iterator.getKey();
        }

        public boolean isDone() {
            return iterator.isDone();
        }

        public Document getDocument() throws IOException {
            String key = iterator.getKey();
            DataStream stream = iterator.getValueStream();
            return decodeDocument(key, stream);
        }

        public boolean nextDocument() throws IOException {
            return iterator.nextKey();
        }

        Document decodeDocument(String key, DataStream stream) throws IOException {
            VByteInput input = new VByteInput(stream);
            Document document = new Document();

            // The first string is the document text, followed by
            // key/value metadata pairs.
            document.identifier = key;
            document.text = input.readString();
            document.metadata = new HashMap<String, String>();

            while (!stream.isDone()) {
                String mapKey = input.readString();
                String mapValue = input.readString();

                document.metadata.put(mapKey, mapValue);
            }

            return document;
        }
    }
}
