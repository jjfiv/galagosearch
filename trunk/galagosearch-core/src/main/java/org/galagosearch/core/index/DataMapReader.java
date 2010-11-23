// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import org.galagosearch.core.types.DataMapItem;
import org.galagosearch.tupleflow.StreamCreator;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.VByteInput;

/**
 * Reads a Map of longs to bytes to a binary file.
 * 
 * Intended for storing a set unique keys each mapping to a value
 * Data Storage: [key:value] 
 * Index: [BlockOffset:FirstKeyInBlock]
 * 
 * Reader must store the entire index in memory
 * The block size in writer can be adjusted to reduce memory usage
 * Alternatively memory usage can be avoid by using linear scans
 * 
 * @author sjh
 */
public class DataMapReader {

  private static class Block {
    public long fileOffset;
    public byte[] lastKey;
  }
  
  File dataFile;
  File indexFile;
  RandomAccessFile dataInput;
  RandomAccessFile indexInput;
  ArrayList<Block> index;
  boolean ramBuffer;
  long indexLength;
  long dataLength;

  public DataMapReader(String folder, String prefix, boolean ramBuffer) throws IOException {
    this.ramBuffer = ramBuffer;

    String datafile = folder + "/" + prefix + ".data";
    String indexfile = folder + "/" + prefix + ".index";
    dataFile = new File(datafile);
    indexFile = new File(indexfile);

    // open the data file //
    dataInput = StreamCreator.inputStream(datafile);
    dataInput.seek(dataInput.length() - 16);
    dataLength = dataInput.readLong();
    if (dataInput.readLong() != DataMapWriter.D_MAGIC_NUMBER) {
      throw new IOException(datafile + " not a valid Bulk Tree Data File!");
    }
    dataInput.seek(0);

    // open and read the index file //

    indexInput = StreamCreator.inputStream(indexfile);
    indexInput.seek(indexInput.length() - 16);
    indexLength = indexInput.readLong();
    if (indexInput.readLong() != DataMapWriter.I_MAGIC_NUMBER) {
      throw new IOException(indexfile + " not a valid Bulk Tree Index File!");
    }
    indexInput.seek(0);

    if (ramBuffer) {
      index = loadRAMIndex(indexInput, indexLength);
    }
  }

  public DataMapItem get(byte[] key) throws IOException {

    // first find the appropriate block
    Block b = getDataBlock(key);
    if (b == null) {
      return null;
    }

    // get a data iterator starting at this offset
    dataInput.seek(b.fileOffset);

    DataMapItem result = null;
    while (dataInput.getFilePointer() < dataLength) {
      result = readData(dataInput);
      if (Utility.compare(result.key, key) >= 0) {
        break;
      }
    }
    if (Utility.compare(result.key, key) == 0) {
      return result;
    }

    // otherwise:
    return null;
  }

  private Block getDataBlock(byte[] key) throws IOException {

    if (ramBuffer) { // we have a memory based index

      int big = index.size() - 1;
      int small = 0;


      while (big - small > 1) {
        int middle = small + (big - small) / 2;

        if (Utility.compare(index.get(middle).lastKey, key) >= 0) {
          big = middle;
        } else {
          small = middle;
        }
      }

      if (Utility.compare(index.get(small).lastKey, key) >= 0) {
        return index.get(small);
      } else if (Utility.compare(index.get(big).lastKey, key) >= 0) {
        return index.get(big);
      } else {
        return null;
      }

    } else { // we are not using memory -- use the file iterator.

      Block b = null;
      indexInput.seek(0);
      while (indexInput.getFilePointer() < indexLength) {
        b = readIndex(indexInput);
        if (Utility.compare(b.lastKey, key) >= 0) {
          return b;
        }
      }
      return null;
    }
  }


  // STATIC functions to read data items from the input data streams

  private static ArrayList<Block> loadRAMIndex(RandomAccessFile indexInput, long length) throws IOException {
    ArrayList<Block> index = new ArrayList();

    Block b;
    indexInput.seek(0);
    while (indexInput.getFilePointer() < length) {
      b = readIndex(indexInput);
      index.add(b);
    }

    return index;
  }

  public static DataMapItem readData(DataInput file) throws IOException {

    VByteInput input = new VByteInput(file);
    
    DataMapItem dmi = new DataMapItem();

    // get key
    int keyLen = input.readInt();
    dmi.key = new byte[keyLen];
    input.readFully(dmi.key);

    // get value
    int valueLen = input.readInt();
    dmi.value = new byte[valueLen];
    input.readFully(dmi.value);

    return dmi;
  }

  public static Block readIndex(DataInput file) throws IOException {
  
    VByteInput input = new VByteInput(file);
    
    Block b = new Block();

    b.fileOffset = input.readLong();

    int keyLen = input.readInt();
    b.lastKey = new byte[keyLen];
    input.readFully(b.lastKey);

    return b;
  }


  
  /*
   * Data iterator
   * Returns items in increasing key order
   */
  public Iterator getIterator() throws IOException {
    return new Iterator(dataFile.getAbsolutePath(), dataLength);
  }

  public class Iterator {
    
    String file;
    RandomAccessFile input;
    long inputLength;
    DataMapItem dmi;

    public Iterator(String file, long length) throws IOException {
      this.file = file;
      inputLength = length;
      reset();
    }

    public void reset() throws IOException {
      input = StreamCreator.inputStream(file);
      nextRecord();
    }

    public DataMapItem getItem() throws IOException {
      return dmi;
    }

    public String getKey() {
      throw new RuntimeException("BulkTreeReader.Iterator.getKey() is not implemented.\nKeys are byte arrays not strings.\n");
    }

    public String getRecordString() {
      return Utility.makeString(dmi.key) + ", " + Utility.makeString(dmi.value);
    }

    public boolean nextRecord() throws IOException {
      if(input.getFilePointer() < inputLength){
        dmi = readData(input);
        return true;
      } else {
        return false;
      }
    }
  }
}
