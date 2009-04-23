// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.DataInput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.CountIterator;
import org.galagosearch.core.retrieval.structured.ExtentIterator;
import org.galagosearch.core.retrieval.structured.IndexIterator;
import org.galagosearch.core.util.ExtentArray;
import org.galagosearch.tupleflow.BufferedFileDataStream;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.VByteInput;

/**
 * Reads a simple positions-based index, where each inverted list in the
 * index contains both term count information and term position information.
 * The term counts data is stored separately from term position information for
 * faster query processing when no positions are needed.
 * 
 * For now, the iterator loads everything into memory before starting query 
 * processing, which is not a workable solution for larger collections.
 * 
 * @author trevor
 */
public class PositionIndexReader implements StructuredIndexPartReader {
    public class Iterator extends ExtentIterator implements IndexIterator {
        int documentCount;
        int totalPositionCount;
        VByteInput documents;
        VByteInput counts;
        VByteInput positions;
        int documentIndex;
        int currentDocument;
        int currentCount;
        ExtentArray extentArray;
        IndexReader.Iterator iterator;

        Iterator(IndexReader.Iterator iterator) throws IOException {
            this.iterator = iterator;
            load();
        }

        private void load() throws IOException {
            long startPosition = iterator.getValueStart();
            long endPosition = iterator.getValueEnd();

            RandomAccessFile input = reader.getInput();
            input.seek(startPosition);
            DataInput stream = new VByteInput(reader.getInput());

            int options = stream.readInt();
            documentCount = stream.readInt();
            totalPositionCount = stream.readInt();

            long documentByteLength = stream.readLong();
            long countsByteLength = stream.readLong();
            long positionsByteLength = stream.readLong();

            long documentStart = input.getFilePointer();
            long documentEnd = documentStart + documentByteLength;

            long countsStart = documentEnd;
            long countsEnd = countsStart + countsByteLength;

            long positionsStart = countsEnd;
            long positionsEnd = positionsStart + positionsByteLength;

            assert positionsEnd == endPosition;

            // create streams for each kind of data
            documents = new VByteInput(new BufferedFileDataStream(input, documentStart, documentEnd));
            counts = new VByteInput(new BufferedFileDataStream(input, countsStart, countsEnd));
            positions = new VByteInput(new BufferedFileDataStream(input, positionsStart, positionsEnd));

            extentArray = new ExtentArray();
            documentIndex = 0;
            loadExtents();
        }

        private void loadExtents() throws IOException {
            currentDocument += documents.readInt();
            currentCount = counts.readInt();
            extentArray.reset();

            int position = 0;
            for (int i = 0; i < currentCount; i++) {
                position += positions.readInt();
                extentArray.add(currentDocument, position, position + 1);
            }
        }
        
        public String getRecordString() {
            StringBuilder builder = new StringBuilder();
            
            builder.append(iterator.getKey());
            builder.append(",");
            builder.append(currentDocument);
            for (int i = 0; i < extentArray.getPosition(); ++i) {
                builder.append(",");
                builder.append(extentArray.getBuffer()[i].begin);
            }
            
            return builder.toString();
        }

        public void reset() throws IOException {
            currentDocument = 0;
            currentCount = 0;
            extentArray.reset();

            load();
        }

        public long getByteLength() throws IOException {
            return iterator.getValueLength();
        }

        public String getCurrentTerm() throws IOException {
            return iterator.getKey();
        }

        public void nextDocument() throws IOException {
            documentIndex += 1;

            if (!isDone()) {
                loadExtents();
            }
        }

        public boolean nextRecord() throws IOException {
            nextDocument();
            if (!isDone()) return true;
            if (iterator.nextKey()) {
                reset();
                return true;
            }
            return false;
        }

        public boolean isDone() {
            return documentIndex >= documentCount;
        }

        public ExtentArray extents() {
            return extentArray;
        }

        public int document() {
            return currentDocument;
        }

        public int count() {
            return currentCount;
        }
    }
    IndexReader reader;

    public PositionIndexReader(IndexReader reader) throws IOException {
        this.reader = reader;
    }
    
    public PositionIndexReader(String pathname) throws FileNotFoundException, IOException {
        reader = new IndexReader(pathname);
    }

    /**
     * Returns an iterator pointing at the first term in the index.
     */
    public Iterator getIterator() throws IOException {
        return new Iterator(reader.getIterator());
    }

    /**
     * Returns an iterator pointing at the specified term, or 
     * null if the term doesn't exist in the inverted file.
     */
    public Iterator getTermExtents(String term) throws IOException {
        IndexReader.Iterator iterator = reader.getIterator(term);

        if (iterator != null) {
            return new Iterator(iterator);
        }
        return null;
    }

    List<Processor<Document>> transformations() {
        return DocumentTransformationFactory.instance(reader.getManifest());
    }

    List<Processor<Document>> transformations(String field) {
        return transformations();
    }

    public void close() throws IOException {
        reader.close();
    }

    public Map<String, NodeType> getNodeTypes() {
        HashMap<String, NodeType> types = new HashMap<String, NodeType>();
        types.put("counts", new NodeType(Iterator.class));
        types.put("extents", new NodeType(Iterator.class));
        return types;
    }

    public IndexIterator getIterator(Node node) throws IOException {
        // TODO(strohman): handle stemming!!
        return getTermExtents(node.getDefaultParameter("term"));
    }
}
