package org.galagosearch.core.index;

import java.io.IOException;
import java.io.OutputStream;
import org.galagosearch.tupleflow.Parameters;

public class ExtentListBuffer implements IndexElement {

    public ExtentListBuffer() {
        this(new Parameters());
    }

    public ExtentListBuffer(Parameters parameters) {
        super();
        documentCount = 0;
        extentCount = 0;
        endCount = 0;
        data = new CompressedRawByteBuffer();
        header = new CompressedByteBuffer();
        documentExtents = new CompressedByteBuffer();
        options = 0; // default

        // configuration-dependent
        boolean skipping = Boolean.parseBoolean(parameters.get("skipping", "true"));
        if (skipping) {
            skips = new CompressedRawByteBuffer();
            skipDistance = (int) parameters.get("skipDistance", 500);
            options |= ValueIterator.HAS_SKIPS;
            numSkips = 0;
        } else {
            skips = null;
        }
    }

    public long dataLength() {
        long listLength = 0;
        listLength += header.length();
        listLength += data.length();
        if (skips != null) {
            listLength += skips.length();
        }
        return listLength;
    }

    public void write(final OutputStream output) throws IOException {
        header.write(output);
        data.write(output);
        header.clear();
        data.clear();

        if (skips != null && skips.length() > 0) {
            skips.write(output);
            skips.clear();
        }
    }

    public byte[] key() {
        return word;
    }

    public void setWord(byte[] word) {
        this.word = word;
        this.lastDocument = 0;
        this.lastPosition = 0;
        this.extentCount = 0;
        if (skips != null) {
            this.lastSkippedDocument = 0;
            this.lastSkippedBytePosition = 0;
        }
    }

    void setValue(long value) {
        this.value = value;
    }

    private void storePreviousDocument() throws IOException {
        if (data.length() > 0) {
            data.add(extentCount);
            data.add(documentExtents);
            documentExtents.clear();
        }
    }

    private void updateSkipInformation() throws IOException {
        // make a skip if we've hit the insertion point
        if (documentCount % skipDistance == 0) {
            // add d-gap on the doc
            skips.add(lastDocument - lastSkippedDocument);
            lastSkippedDocument = lastDocument;

            // and on the byte position
            skips.add(data.length() - lastSkippedBytePosition);
            lastSkippedBytePosition = data.length();
            numSkips++;
        }
    }

    public void addDocument(long documentID) throws IOException {
        storePreviousDocument();
        documentCount++;
        if (skips != null) updateSkipInformation();
        data.add(documentID - lastDocument);
        lastDocument = documentID;
        lastStartExtent = 0;
        extentCount = 0;
        endCount = 0;
        value = 0;
    }

    public void addBegin(int begin) {
        extentCount++;
        documentExtents.add(begin - lastStartExtent);
        lastStartExtent = begin;
    }

    public void addEnd(int end) {
        endCount++;
        // Record multiple extents at the same begin location
        if (endCount > extentCount) {
            documentExtents.add(0);
            extentCount++;
        }
        documentExtents.add(end - lastStartExtent);
        documentExtents.add((long) value);
    }

    public void close() throws IOException {
        storePreviousDocument();

        // remove skip options if the buffer is empty
        if (skips != null && skips.length() == 0) {
            options = (0xffff - ValueIterator.HAS_SKIPS) & options;
        }

        header.add(options);
        header.add(documentCount);

        if (skips != null && skips.length() > 0) {
            header.add(skipDistance);
            header.add(numSkips);
            header.add(data.length());
        }

    }
    private long value;
    private long lastDocument;
    private int lastStartExtent;
    private int lastPosition;
    private int extentCount;
    private int endCount;
    private int documentCount;
    private int options;
    public byte[] word;
    public CompressedByteBuffer header;
    public CompressedRawByteBuffer data;
    public CompressedByteBuffer documentExtents;
    // To support skipping
    public CompressedRawByteBuffer skips;
    int skipDistance;
    int numSkips;
    long lastSkippedDocument;
    long lastSkippedBytePosition;
}
