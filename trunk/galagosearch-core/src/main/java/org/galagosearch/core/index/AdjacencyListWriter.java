 package org.galagosearch.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import org.galagosearch.core.types.Adjacency;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 *
 * @author irmarc
 */
@InputClass(className = "org.galagosearch.core.types.Adjacency", order={"+source", "+destination"})
public class AdjacencyListWriter implements Adjacency.SourceDestinationOrder.ShreddedProcessor, Processor<Adjacency> {

  public class InvertedList implements IndexElement {

    CompressedRawByteBuffer data = new CompressedRawByteBuffer();
    CompressedByteBuffer header = new CompressedByteBuffer();
    int numNeighbors;
    int lastID;
    byte[] word;

    public InvertedList(byte[] word) {
      this.word = word;
      this.numNeighbors = 0;
      this.lastID = 0;
    }

    public void write(final OutputStream stream) throws IOException {
      header.write(stream);
      header.clear();
      data.write(stream);
      data.clear();
    }

    public void addDestination(byte[] destination) throws IOException {
      int converted = Utility.toInt(destination);
      data.add(converted - lastID);
      lastID = converted;
      numNeighbors++;
    }

    public void addWeight(double weight) throws IOException {
      data.addDouble(weight);
    }

    public byte[] key() {
      return word;
    }

    public long dataLength() {
      return data.length() + header.length();
    }

    public void close() {
      header.add(numNeighbors);
    }
  }
  IndexWriter writer;
  InvertedList list = null;

  /** Creates a new instance of AdjacencyListWriter */
  public AdjacencyListWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    writer = new IndexWriter(parameters);
    writer.getManifest().add("readerClass", AdjacencyListReader.class.getName());
    writer.getManifest().add("writerClass", getClass().getName());
  }

  public void processSource(byte[] source) throws IOException {
    if (list != null) {
      list.close();
      writer.add(list);
    }

    list = new InvertedList(source);
  }

  public void processDestination(byte[] destination) throws IOException {
    list.addDestination(destination);
  }

  public void processTuple(double weight) throws IOException {
    list.addWeight(weight);
  }

  // just for this method
  byte[] lastSource = null;
  public void process(Adjacency object) throws IOException {
    if (lastSource == null ||
            Utility.compare(lastSource, object.source) != 0) {
      if (list != null) {
        list.close();
        writer.add(list);
      }
      
      list = new InvertedList(object.source);
      lastSource = object.source;
    }
    list.addDestination(object.destination);
    list.addWeight(object.weight);
  }

  public void close() throws IOException {
    if (list != null) {
      list.close();
      writer.add(list);
    }
    writer.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getXML().containsKey("filename")) {
      handler.addError("PositionIndexWriter requires a 'filename' parameter.");
      return;
    }

    String index = parameters.getXML().get("filename");
    Verification.requireWriteableFile(index, handler);
  }
}
