package org.galagosearch.core.mergeindex.parallel;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class NumberMappingReader {
  String mappingFolder;
  HashMap<Integer, ByteBuffer> fileReaders = new HashMap();

  public NumberMappingReader(Parameters p){
    mappingFolder = p.get("filename");
  }

  public NumberMappingReader(String filename){
    mappingFolder = filename;
  }

  public int getNewDocNumber(int indexId, int oldDocId) throws IOException{
    if( ! fileReaders.containsKey(indexId) ){
      String filename = mappingFolder + File.separator + indexId;
      RandomAccessFile file = new RandomAccessFile(new File(filename), "r");
      FileChannel channel = file.getChannel();
      ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
      fileReaders.put(indexId, buffer);
    }

    ByteBuffer buffer = fileReaders.get(indexId);
    
    assert (oldDocId * 4) < buffer.capacity();

    return buffer.getInt(oldDocId * 4);
  }
}
