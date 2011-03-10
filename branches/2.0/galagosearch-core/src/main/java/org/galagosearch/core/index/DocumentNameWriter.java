// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Sorter;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;

/**
 * 
 * Writes a mapping from document names to document numbers
 * 
 * Does not assume that the data is sorted
 *  - as data would need to be sorted into both key and value order
 *  - instead this class takes care of the re-sorting
 *  - this may be inefficient, but docnames is a relatively small pair of files
 *
 * @author sjh
 */
@InputClass(className = "org.galagosearch.core.types.NumberedDocumentData")
public class DocumentNameWriter implements Processor<NumberedDocumentData> {
  Sorter<KeyValuePair> sorterFL;
  Sorter<KeyValuePair> sorterRL;
  
  NumberedDocumentData last = null;
  Counter documentNamesWritten = null;

  public DocumentNameWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    //writer = new BulkTreeWriter(parameters);
    documentNamesWritten = parameters.getCounter("Document Names Written");
    // make a folder
    String fileName = parameters.getXML().get("filename");
    
    Parameters p = new Parameters();
    p.copy(parameters.getXML());
    p.set("order", "forward");
    IndexWriterProcessor writerFL = new IndexWriterProcessor(fileName, p);
    p.set("order", "backward");
    IndexWriterProcessor writerRL = new IndexWriterProcessor(fileName + ".reverse", p);
    
    sorterFL = new Sorter<KeyValuePair>(new KeyValuePair.KeyOrder());
    sorterRL = new Sorter<KeyValuePair>(new KeyValuePair.KeyOrder());
    sorterFL.processor = writerFL;
    sorterRL.processor = writerRL;
      
  }

  public void process(int number, String identifier) throws IOException {
    byte[] docnum = Utility.fromInt(number);
    byte[] docname = Utility.fromString(identifier);

    // numbers -> names
    KeyValuePair btiFL = new KeyValuePair(docnum, docname);
    sorterFL.process(btiFL);

    // names -> numbers
    KeyValuePair btiRL = new KeyValuePair(docname, docnum);
    sorterRL.process(btiRL);

    if (documentNamesWritten != null) documentNamesWritten.increment();
  }

  public void process(NumberedDocumentData ndd) throws IOException {
     if(last == null) last = ndd;

    assert last.number <= ndd.number;
    assert last.identifier != null;
    process(ndd.number, ndd.identifier);
  }

  public void close() throws IOException {
    sorterFL.close();
    sorterRL.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getXML().containsKey("filename")) {
      handler.addError("DocumentNameWriter requires a 'filename' parameter.");
      return;
    }
  }



  /*
   * Translates the Key Value Pairs to Generic elements + writes them to the index
   */
  private class IndexWriterProcessor implements Processor<KeyValuePair>{
    IndexWriter writer;
    public IndexWriterProcessor(String fileName, Parameters p) throws IOException{
      // default uncompressed index is fine
      writer = new IndexWriter(fileName, p);
      writer.getManifest().set("writerClass", getClass().getName());
      writer.getManifest().set("readerClass", DocumentNameReader.class.getName());
    }
    
    public void process(KeyValuePair kvp) throws IOException {
      GenericElement element = new GenericElement(kvp.key, kvp.value);
      writer.add(element);
    }

    public void close() throws IOException {
      writer.close();
    }
    
  }
}
