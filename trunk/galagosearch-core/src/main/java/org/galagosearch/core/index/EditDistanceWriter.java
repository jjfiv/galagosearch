 package org.galagosearch.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import org.galagosearch.core.types.Adjacency;
import org.galagosearch.core.util.DoubleCodec;
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
@InputClass(className = "org.galagosearch.core.types.Adjacency", order={"+source", "+weight"})
public class EditDistanceWriter implements Adjacency.SourceWeightOrder.ShreddedProcessor {

  public class InvertedList implements IndexElement {

    CompressedRawByteBuffer data = new CompressedRawByteBuffer();
    CompressedRawByteBuffer edits = null;
    CompressedByteBuffer header = new CompressedByteBuffer();
    int editCount;
    int numNeighbors;
    int lastID;
    byte[] word;

    public InvertedList(byte[] word) {
      this.word = word;
      this.numNeighbors = 0;
    }

    public void write(final OutputStream stream) throws IOException {
      header.write(stream);
      header.clear();
      data.write(stream);
      data.clear();
    }

    public void addWeight(double weight) throws IOException {
      if (edits != null) {
	  data.add(editCount);
	  data.add(edits);
      }
      int i = (int) Math.round(weight);
      data.add(i);
      editCount = 0;
      if (edits == null) {
	edits = new CompressedRawByteBuffer();
      } else {
	edits.clear();
      }
    }

    public void addDestination(byte[] bytes) throws IOException {
      edits.add(bytes.length);
      edits.add(bytes);
      editCount++;
      numNeighbors++;
    }

    public byte[] key() {
      return word;
    }

    public long dataLength() {
      return data.length() + header.length();
    }

    public void close() throws IOException {
      if (edits != null) {
	  data.add(editCount);
	  data.add(edits);
      }
      edits.clear();
      header.add(numNeighbors);
    }
  }
  IndexWriter writer;
  InvertedList list = null;

  /** Creates a new instance of AdjacencyListWriter */
  public EditDistanceWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    writer = new IndexWriter(parameters);
    writer.getManifest().add("readerClass", EditDistanceReader.class.getName());
    writer.getManifest().add("writerClass", getClass().getName());
    writer.getManifest().add("part", parameters.getXML().get("part"));
    writer.getManifest().add("name", parameters.getXML().get("name"));
  }

  public void processSource(byte[] source) throws IOException {
    if (list != null) {
      list.close();
      writer.add(list);
    }

    list = new InvertedList(source);
  }

  // SourceWeightOrder.ShreddedProcessor
  public void processWeight(double weight) throws IOException {
    list.addWeight(weight);
  }

  public void processTuple(byte[] destination) throws IOException {
    list.addDestination(destination);
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
