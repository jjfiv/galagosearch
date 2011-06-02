// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;

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
 * @author trevor, sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.DocumentSplit")
@OutputClass(className = "org.galagosearch.core.parse.Document")
public class UniversalParser extends StandardStep<DocumentSplit, Document> {
  private Counter documentCounter;
  private Parameters parameters;
  private long count = 0;
  private Logger LOG = Logger.getLogger(getClass().toString());


  public UniversalParser(TupleFlowParameters parameters) {
    documentCounter = parameters.getCounter("Documents Parsed");
    this.parameters = parameters.getXML();
  }

  static private void bzipHeaderCheck(BufferedInputStream stream) throws IOException {
    char[] header = new char[2];
    stream.mark(4);
    header[0] = (char) stream.read();
    header[1] = (char) stream.read();
    String hdrStr = new String(header);
    if (hdrStr.equals("BZ") == false) 
      stream.reset();
  }

  static public BufferedReader getBufferedReader(DocumentSplit split) throws IOException {
    FileInputStream stream = StreamCreator.realInputStream(split.fileName);
    BufferedReader reader;

    if (split.isCompressed) {
      // Determine compression type
      if (split.fileName.endsWith("gz")) { // Gzip
        reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(stream)));
      } else { // BZip2
        BufferedInputStream bis = new BufferedInputStream(stream);
        bzipHeaderCheck(bis);
        reader = new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(bis)));
      }
    } else {
      reader = new BufferedReader(new InputStreamReader(stream));
    }
    return reader;
  }

  public BufferedInputStream getBufferedInputStream(DocumentSplit split) throws IOException {
    FileInputStream fileStream = StreamCreator.realInputStream(split.fileName);
    BufferedInputStream stream;

    if (split.isCompressed) {
      // Determine compression algorithm
      if (split.fileName.endsWith("gz")) { // Gzip
        stream = new BufferedInputStream(new GZIPInputStream(fileStream));
      } else { // bzip2
        BufferedInputStream bis = new BufferedInputStream(fileStream);
        bzipHeaderCheck(bis);
        stream = new BufferedInputStream(new BZip2CompressorInputStream(bis));
      }
    } else {
      stream = new BufferedInputStream(fileStream);
    }
    return stream;
  }

  

  public void process(DocumentSplit split) throws IOException {
    DocumentStreamParser parser;

    if (split.fileType.equals("html") ||
        split.fileType.equals("xml") ||
        split.fileType.equals("txt")) {
      parser = new FileParser(parameters, split.fileName, getBufferedReader(split));
    } else if (split.fileType.equals("arc")) {
      parser = new ArcParser(getBufferedInputStream(split));
    } else if (split.fileType.equals("warc")) {
      parser = new WARCParser(getBufferedInputStream(split));
    } else if (split.fileType.equals("trectext")) {
      parser = new TrecTextParser(getBufferedReader(split));
    } else if (split.fileType.equals("trecweb")) {
      parser = new TrecWebParser(getBufferedReader(split));
    } else if (split.fileType.equals("twitter")) {
      parser = new TwitterParser(getBufferedReader(split));
    } else if (split.fileType.equals("corpus")) {
      parser = new IndexReaderSplitParser(split);
    } else if (split.fileType.equals("wiki")) {
      parser = new WikiParser(getBufferedReader(split));
    } else {
      throw new IOException("Unknown fileType: " + split.fileType +
          " for fileName: "  + split.fileName);
    }

    Document document;
    while ((document = parser.nextDocument()) != null) {
      count++;
      document.fileId = split.fileId;
      document.totalFileCount = split.totalFileCount;
      processor.process(document);
      if (documentCounter != null)
        documentCounter.increment();
    }
  }
}
