package org.galagosearch.core.index;

import java.io.IOException;
import java.io.OutputStream;

public class ExtentListBuffer implements IndexElement {
    public ExtentListBuffer() {
        super();
        documentCount = 0;
        extentCount = 0;
        endCount = 0;
        data = new CompressedByteBuffer();
        header = new CompressedByteBuffer();
        documentExtents = new CompressedByteBuffer();
    }

    public long dataLength() {
        long listLength = 0;
        listLength += header.length();
        listLength += data.length();
        return listLength;
    }

    public void write(final OutputStream output) throws IOException {
        output.write(header.getBytes(), 0, header.length());
        output.write(data.getBytes(), 0, data.length());
    }

    public byte[] key() {
        return word;
    }

    public void setWord(byte[] word) {
        this.word = word;
        this.lastDocument = 0;
        this.lastPosition = 0;
        this.extentCount = 0;
    }

    void setValue(long value) {
        this.value = value;
    }

    private void storePreviousDocument() {
        if (data.position > 0) {
            data.add(extentCount);
            data.add(documentExtents);
            documentExtents.clear();
        }
    }

    public void addDocument(long documentID) {
        storePreviousDocument();
        data.add(documentID - lastDocument);
        lastDocument = documentID;
        lastStartExtent = 0;
        extentCount = 0;
        endCount = 0;
        value = 0;
        documentCount++;
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
        documentExtents.add(value);
    }

    public void close() {
        int options = 0;
        storePreviousDocument();
        header.add(options);
        header.add(documentCount);
    }

    private long value;
    private long lastDocument;
    private int lastStartExtent;
    private int lastPosition;
    private int extentCount;
    private int endCount;
    private int documentCount;
    public byte[] word;
    public CompressedByteBuffer header;
    public CompressedByteBuffer data;
    public CompressedByteBuffer documentExtents;
}
