// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.galagosearch.core.types.DataMapItem;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.InputClass;
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
 *
 * @author sjh
 */
@InputClass(className = "org.galagosearch.core.types.NumberedDocumentData")
public class DocumentNameWriter implements Processor<NumberedDocumentData> {
  Sorter<DataMapItem> sorterFL;
  Sorter<DataMapItem> sorterRL;
  
  NumberedDocumentData last = null;
  Counter documentNamesWritten = null;

  public DocumentNameWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    //writer = new BulkTreeWriter(parameters);
    documentNamesWritten = parameters.getCounter("Document Names Written");
    // make a folder
    File folder = new File(parameters.getXML().get("filename"));
    if(!folder.isDirectory()){
      folder.delete();
      folder.mkdirs();
    }
    
    DataMapWriter writerFL = new DataMapWriter(folder, "fl");
    DataMapWriter writerRL = new DataMapWriter(folder, "rl");
    
    sorterFL = new Sorter<DataMapItem>(new DataMapItem.KeyOrder());
    sorterRL = new Sorter<DataMapItem>(new DataMapItem.KeyOrder());
    sorterFL.processor = writerFL;
    sorterRL.processor = writerRL;
      
  }

  public void process(NumberedDocumentData ndd) throws IOException {
     if(last == null) last = ndd;

    assert last.number <= ndd.number;
    assert last.identifier != null;
   
    byte[] docnum = Utility.makeBytes(ndd.number);
    byte[] docname = Utility.makeBytes(ndd.identifier);
    
    DataMapItem btiFL = new DataMapItem(docnum, docname);
    sorterFL.process(btiFL);

    DataMapItem btiRL = new DataMapItem(docname, docnum);
    sorterRL.process(btiRL);

    if (documentNamesWritten != null) documentNamesWritten.increment();
  }

  public void close() throws IOException {
    sorterFL.close();
    sorterRL.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getXML().containsKey("filename")) {
      handler.addError("DocumentNameWriter requires an 'filename' parameter.");
      return;
    }
  }
}
