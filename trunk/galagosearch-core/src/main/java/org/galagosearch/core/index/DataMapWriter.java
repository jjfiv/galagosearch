// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.galagosearch.core.types.DataMapItem;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.StreamCreator;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;

/**
 * Writes a Map of longs to bytes to a binary file.
 * 
 * Intended for storing a mapping of keys to values
 * Data Storage: [key:value] 
 * Index: [fileOffset:FirstKeyInBlock]
 * 
 * Reader must store the entire index in memory
 * The block size can be adjusted to reduce memory usage
 * 
 * @author sjh
 */
@InputClass(className = "org.galagosearch.core.types.DataMapItem", order = {"+key"})
public class DataMapWriter implements Processor<DataMapItem> {
  public static final long D_MAGIC_NUMBER = 0x2747592838585934L;
  public static final long I_MAGIC_NUMBER = 0x1838483759494956L;

  
  DataMapBuffer outputBuffer;
  DataMapBuffer indexBuffer; 
  DataOutputStream outputStream;
  DataOutputStream indexStream;

  // blocks store a fixed number of kv pairs
  int blockSize = 2048; // number of key/value pairs in each block
  byte[] blockKey; // last key of current block
  long fileOffset; // bytes written to file
  int curBlockCount; // bytes in current block

  long totalKeys = 0;
  
  public DataMapWriter(File folder, String prefix) throws FileNotFoundException, IOException {
    String indexfile = folder + "/" + prefix + ".index"  ;
    String datafile = folder + "/" + prefix + ".data"  ;
    
    outputStream = StreamCreator.realOutputStream(datafile); 
    indexStream = StreamCreator.realOutputStream(indexfile); 
    outputBuffer = new DataMapBuffer(outputStream, 1024 * 1024); // 1MB buffer
    indexBuffer = new DataMapBuffer(indexStream, 1024 * 1024); // 1MB buffer

    blockKey = null;
    fileOffset = outputBuffer.length(); 
    curBlockCount = 0;
  }

  public void process(DataMapItem dmi) throws IOException {
    assert (blockKey == null || Utility.compare(blockKey, dmi.key) <= 0);
    blockKey = dmi.key;
    
    outputBuffer.add(dmi.key.length);
    outputBuffer.addBytes(dmi.key);
    outputBuffer.add(dmi.value.length);
    outputBuffer.addBytes(dmi.value);

    curBlockCount += 1;
    if (curBlockCount >= blockSize){
      // the this block is full; write the index data
      flushIndex();
      // reset the counter
      curBlockCount = 0;
    }

    totalKeys++;
  }

  private void flushIndex() throws IOException{
    indexBuffer.add(fileOffset);
    indexBuffer.add(blockKey.length);
    indexBuffer.addBytes(blockKey);

    fileOffset = outputBuffer.length();
  }
  
  public void close() throws IOException {
    flushIndex();

    outputBuffer.flush();
    indexBuffer.flush();

    outputStream.writeLong(outputBuffer.length());
    outputStream.writeLong(D_MAGIC_NUMBER);
    
    indexStream.writeLong(indexBuffer.length());
    indexStream.writeLong(I_MAGIC_NUMBER);
    
    outputBuffer.close();
    indexBuffer.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getXML().containsKey("filename")) {
      handler.addError("DocumentNameWriter requires an 'filename' parameter.");
      return;
    }
  }
}
