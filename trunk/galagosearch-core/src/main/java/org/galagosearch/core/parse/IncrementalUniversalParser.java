// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.parse;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.util.LinkedList;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.galagosearch.tupleflow.StreamCreator;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.tupleflow.Parameters;

/**
 * 
 * @author trevor, sjh, schiu
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.DocumentSplit")
@OutputClass(className = "org.galagosearch.core.parse.Document")
public class IncrementalUniversalParser extends StandardStep<DocumentSplit, Document> {

    private Counter documentCounter;
    private Parameters parameters;
    private long count = 0;
    private final Logger LOG = Logger.getLogger(getClass().toString());
    private LinkedList<DocumentSplit> splits = new LinkedList<DocumentSplit>();
    private DocumentSplit currentSplit;
    private DocumentStreamParser currentParser;
    private int incrementAmount = 1000;

    public IncrementalUniversalParser(TupleFlowParameters parameters) {
        //documentCounter = parameters.getCounter("Documents Parsed");
        this.parameters = parameters.getXML();
    }

    private void bzipHeaderCheck(BufferedInputStream stream) throws IOException {
        char[] header = new char[2];
        stream.mark(4);
        header[0] = (char) stream.read();
        header[1] = (char) stream.read();
        String hdrStr = new String(header);
        if (hdrStr.equals("BZ") == false) {
            stream.reset();
        }
    }

    public BufferedReader getBufferedReader(DocumentSplit split)
            throws IOException {
        FileInputStream stream = StreamCreator.realInputStream(split.fileName);
        BufferedReader reader;

        if (split.isCompressed) {
            // Determine compression type
            if (split.fileName.endsWith("gz")) { // Gzip
                reader = new BufferedReader(new InputStreamReader(
                        new GZIPInputStream(stream)));
            } else { // BZip2
                BufferedInputStream bis = new BufferedInputStream(stream);
                bzipHeaderCheck(bis);
                reader = new BufferedReader(new InputStreamReader(
                        new BZip2CompressorInputStream(bis)));
            }
        } else {
            reader = new BufferedReader(new InputStreamReader(stream));
        }
        return reader;
    }

    public BufferedInputStream getBufferedInputStream(DocumentSplit split)
            throws IOException {
        FileInputStream fileStream = StreamCreator.realInputStream(split.fileName);
        BufferedInputStream stream;

        if (split.isCompressed) {
            // Determine compression algorithm
            if (split.fileName.endsWith("gz")) { // Gzip
                stream = new BufferedInputStream(
                        new GZIPInputStream(fileStream));
            } else { // bzip2
                BufferedInputStream bis = new BufferedInputStream(fileStream);
                bzipHeaderCheck(bis);
                stream = new BufferedInputStream(
                        new BZip2CompressorInputStream(bis));
            }
        } else {
            stream = new BufferedInputStream(fileStream);
        }
        return stream;
    }

    public void process(DocumentSplit inSplit) throws IOException {
        splits.offer(inSplit);
    }

    public void nextParser() throws IOException {
        if(splits.isEmpty()) {
            currentSplit=null;
            currentParser = null;
            return;
        }
        else
            currentSplit = splits.pop();

        if (currentSplit.fileType.equals("html")
                || currentSplit.fileType.equals("xml")
                || currentSplit.fileType.equals("txt")) {
            currentParser = new FileParser(parameters, currentSplit.fileName,
                    getBufferedReader(currentSplit));
        } else if (currentSplit.fileType.equals("arc")) {
            currentParser = new ArcParser(getBufferedInputStream(currentSplit));
        } else if (currentSplit.fileType.equals("warc")) {
            currentParser = new WARCParser(getBufferedInputStream(currentSplit));
        } else if (currentSplit.fileType.equals("trectext")) {
            currentParser = new TrecTextParser(getBufferedReader(currentSplit));
        } else if (currentSplit.fileType.equals("trecweb")) {
            currentParser = new TrecWebParser(getBufferedReader(currentSplit));
        } else if (currentSplit.fileType.equals("twitter")) {
            currentParser = new TwitterParser(getBufferedReader(currentSplit));
        } else if (currentSplit.fileType.equals("corpus")) {
            currentParser = new IndexReaderSplitParser(currentSplit);
        } else if (currentSplit.fileType.equals("wiki")) {
            currentParser = new WikiParser(getBufferedReader(currentSplit));
        } else {
            throw new IOException("Unknown fileType: " + currentSplit.fileType
                    + " for fileName: " + currentSplit.fileName);
        }

    }

    public void increment() throws IOException {
        Document document;
        System.out.println("Incrementing");
        if (currentParser == null) {
            nextParser();
            if (currentParser == null) {
                return;
            }
        }

        for (int i = 0; i < incrementAmount; i++) {
            document = currentParser.nextDocument();
            while (document == null) {
                nextParser();
                if (currentParser == null) {
                    return;
                }
                document = currentParser.nextDocument();
            }

            count++;
            document.fileId = currentSplit.fileId;
            document.totalFileCount = currentSplit.totalFileCount;
            processor.process(document);
            if (documentCounter != null) {
                documentCounter.increment();
            }
        }
    }
}
