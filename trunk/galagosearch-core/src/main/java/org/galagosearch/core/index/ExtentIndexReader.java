// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.DataInput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import org.galagosearch.core.retrieval.*;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.*;
import org.galagosearch.core.util.ExtentArray;
import org.galagosearch.tupleflow.DataStream;
import org.galagosearch.tupleflow.VByteInput;

/**
 *
 * @author trevor
 */
public class ExtentIndexReader implements StructuredIndexPartReader {
    public class Iterator extends ExtentIterator implements IndexIterator {
        IndexReader.Iterator iterator;
        DataInput stream;
        int documentCount;
        int options;
        boolean done;
        int document;
        ExtentArray extents;
        int termDocs;

        public Iterator(IndexReader.Iterator iterator) throws IOException {
            this.iterator = iterator;
            this.extents = new ExtentArray();
            this.done = false;
            loadIndex();
        }

        public void loadIndex() throws IOException {
            readHeader(iterator.getValueStream());
        }

        public void reset() throws IOException {
            done = false;

            document = 0;
            extents.reset();
            documentCount = 0;
            termDocs = 0;

            loadIndex();
        }
        
        public String getRecordString() {
            StringBuilder builder = new StringBuilder();
            builder.append(iterator.getKey());
            builder.append(",");
            builder.append(document);
            for (int i = 0; i < extents.getPosition(); ++i) {
                builder.append(",(");
                builder.append(extents.getBuffer()[i].begin);
                builder.append(",");
                builder.append(extents.getBuffer()[i].end);
                builder.append(")");
           }
           return builder.toString();
        }

        public boolean nextRecord() throws IOException {
            nextDocument();
            if (!isDone()) return true;
            if (iterator.nextKey()) {
                loadIndex();
                return true;
            } else {
                return false;
            }
        }

        private void readHeader(DataStream compressedStream) throws IOException {
            stream = new VByteInput(compressedStream);

            options = stream.readInt();
            documentCount = stream.readInt();
            document = 0;
            termDocs = 0;
            done = false;

            nextDocument();
        }

        public void nextDocument() throws IOException {
            extents.reset();

            if (termDocs >= documentCount) {
                done = true;
                return;
            }

            termDocs++;
            int deltaDocument = stream.readInt();
            document += deltaDocument;

            int extentCount = stream.readInt();
            int begin = 0;

            for (int i = 0; i < extentCount; i++) {
                int deltaBegin = stream.readInt();
                int extentLength = stream.readInt();
                long value = stream.readLong();

                begin = deltaBegin + begin;
                int end = begin + extentLength;

                extents.add(document, begin, end, value);
            }
        }

        public int document() {
            return document;
        }

        public int count() {
            return extents.getPosition();
        }

        public ExtentArray extents() {
            return extents;
        }

        public boolean isDone() {
            return done;
        }
    }
    IndexReader reader;

    public ExtentIndexReader(IndexReader reader) throws FileNotFoundException, IOException {
        this.reader = reader;
    }

    public Iterator getIterator() throws IOException {
        return new Iterator(reader.getIterator());
    }

    public Iterator getExtents(String term) throws IOException {
        IndexReader.Iterator iterator = reader.getIterator(term);

        if (iterator != null) {
            return new Iterator(iterator);
        }
        return null;
    }

    public CountIterator getCounts(String term) throws IOException {
        IndexReader.Iterator iterator = reader.getIterator(term);

        if (iterator != null) {
            return new Iterator(iterator);
        }
        return null;
    }

    public void close() throws IOException {
        reader.close();
    }

    public HashMap<String, NodeType> getNodeTypes() {
        HashMap<String, NodeType> nodeTypes = new HashMap<String, NodeType>();
        nodeTypes.put("extents", new NodeType(Iterator.class));
        return nodeTypes;
    }

    public IndexIterator getIterator(Node node) throws IOException {
        if (node.getOperator().equals("extents")) {
            return getExtents(node.getDefaultParameter());
        } else {
            throw new UnsupportedOperationException("Node type " + node.getOperator() +
                                                    " isn't supported.");
        }
    }
}
