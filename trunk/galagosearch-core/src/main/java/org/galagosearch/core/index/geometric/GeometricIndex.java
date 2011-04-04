// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.geometric;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.galagosearch.core.index.mem.FlushToDisk;
import org.galagosearch.core.index.mem.MemoryIndex;
import org.galagosearch.core.mergeindex.sequential.MergeSequentialIndexShards;
import org.galagosearch.core.parse.NumberedDocument;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;


/*
 *  author: sjh, schui
 *  
 *  Geometric Index Writer updates an in-memory index
 *  periodically the memory index is flushed to disk
 *  depending on the number of indexes on disk, 
 *  some index blocks may then be merged 
 *  
 *  Notes:
 *  document ids are unique throughout the system
 *  merging process should no re-number documents
 *  
 *  indexBlockSize is the number of documents in 
 *  an index block empirically (over trec newswire 
 *  documents), 50000 documents should use between 
 *  500 and 800MB of RAM. Depending on your collection; 
 *  you may want to change this.
 *  
 */
@Verified
@InputClass(className = "org.galagosearch.core.parse.NumberedDocument")
public class GeometricIndex implements Processor<NumberedDocument> {

  private Logger logger = Logger.getLogger(GeometricIndex.class.toString());
  String shardDirectory;
  long globalDocumentCount;
  int indexBlockSize; // measured in documents
  int indexBlockCount;
  boolean stemming;
  String mergeMode;
  MemoryIndex currentMemoryIndex;
  GeometricPartitions geometricParts;
  final Object partsLock = new Object();

  public GeometricIndex(TupleFlowParameters parameters) {

    shardDirectory = parameters.getXML().get("directory");
    stemming = (boolean) parameters.getXML().get("stemming", false);
    mergeMode = parameters.getXML().get("mergeMode", "local");

    // 10,000 is a small testing value -- 50,000 is probably a better choice.
    indexBlockSize = (int) parameters.getXML().get("indexBlockSize", 10000);
    // radix is the number of indexes of each size to store before a merge op
    // keep in mind that the total number of indexes is difficult to control
    int radix = (int) parameters.getXML().get("radix", 3);
    geometricParts = new GeometricPartitions(radix);

    // initialisation
    globalDocumentCount = 0;
    indexBlockCount = 0;

    resetCurrentMemoryIndex();
  }

  public void process(NumberedDocument doc) throws IOException {
    currentMemoryIndex.process(doc);
    globalDocumentCount++; // now one higher than the document just processed
    if (globalDocumentCount % indexBlockSize == 0) {
      flushCurrentIndexBlock();
//      if (mergeMode.equals("threaded")) {
//        maintainMergeThreads();
//      } else {
      maintainMergeLocal();
//      }
    }
  }

  public void close() throws IOException {
    // this will ensure that all data is on disk
    flushCurrentIndexBlock();

    // I also want to perform one final merge after this thread finishes.
    finalMergeFlag = 1;
    /*
    try {
      for (Thread t : mergeThreads) {
        t.join();
      }
    } catch (InterruptedException ex) {
      Logger.getLogger(GeometricIndex.class.getName()).log(Level.SEVERE, null, ex);
    }*/
    logger.info("Performing final merge");
    //int i=1;
    //int max = geometricParts.getMaxSize();

    try {
        doMerge(geometricParts.getAllShards() , new File(shardDirectory + File.separator + "final") );
    } catch (IOException ex) {
    	Logger.getLogger(GeometricIndex.class.getName()).log(Level.SEVERE, null, ex);
    }


  }

  private void flushCurrentIndexBlock() {
    // First check that the final memory index contains some data:
    if (currentMemoryIndex.getCollectionLength() < 1) {
      return;
    }

    logger.info("Flushing current memory Index. id = " + indexBlockCount);

    // may want to wait for the previous thread to complete
    // but not entirely necessary

    final GeometricIndex g = this;
    final MemoryIndex flushingMemoryIndex = currentMemoryIndex;
    final File shardFolder = getNextIndexShardFolder(1);

    // reset the current index for the next document
    resetCurrentMemoryIndex();

/*    if (mergeMode.equals("threaded")) {
      Thread t1 = new Thread() {

        @Override
        public void run() {
          try {
            // first flush the index to disk
            boolean mode = mergeMode.equals("threaded");
            (new FlushToDisk()).flushMemoryIndex(flushingMemoryIndex, shardFolder.getAbsolutePath(), mode);
            // indicate that the flushing part of this thread is done
            flushingMemoryIndex.close();

            // add flushed index to the set of bins -- needs to be a synconeous action
            synchronized (partsLock) {
              geometricParts.add(1, shardFolder.getAbsolutePath());
            }

          } catch (IOException e) {
            logger.severe(e.toString());
          }
        }
      };

      t1.start();

    } else {
*/    try {
        // first flush the index to disk
        (new FlushToDisk()).flushMemoryIndex(flushingMemoryIndex, shardFolder.getAbsolutePath(), false);
        // indicate that the flushing part of this thread is done
        flushingMemoryIndex.close();

        // add flushed index to the set of bins -- needs to be a synconeous action
        synchronized (partsLock) {
          geometricParts.add(1, shardFolder.getAbsolutePath());
        }

      } catch (IOException e) {
        logger.severe(e.toString());
      }
    }

//  }

  // flags for the mergeThread
  int mergeFlag = 0;
  int finalMergeFlag = 0;
  ArrayList<Thread> mergeThreads = new ArrayList();

/*  private void maintainMergeThreads() {

    logger.info("Maintaining Merge Threads");
    //check if we need to start any new threads
    while (geometricParts.getMaxSize() > mergeThreads.size()) {
      mergeThreads.add(startMergeThread(mergeThreads.size() + 1));
    }

    // request a couple of merge attempts at each thread
    int inc = mergeThreads.size();
    mergeFlag = mergeFlag + inc + inc;
  }
*/
  private void maintainMergeLocal() {
    logger.info("Maintaining Merge Local");
    int i = 1;
   // while (i <= geometricParts.getMaxSize()) {
      try {
        mergeIndexShards(i);
      } catch (IOException ex) {
        Logger.getLogger(GeometricIndex.class.getName()).log(Level.SEVERE, null, ex);
      }
  //    i++;
  //  }
  }

/*  private Thread startMergeThread(final int binSize) {
    final GeometricIndex g = this;
    Thread mergeThread = new Thread() {

      @Override
      public void run() {
        try {
          while (true) {
            // sleep 5 seconds
            Thread.sleep(1000);
            if (g.mergeFlag > 0) {
              g.mergeFlag--;
              try {
                g.mergeIndexShards(binSize);
              } catch (IOException ex) {
                Logger.getLogger(GeometricIndex.class.getName()).log(Level.SEVERE, null, ex);
              }
            }
            if (g.finalMergeFlag > 0) {
              try {
                g.mergeIndexShards(binSize);
              } catch (IOException ex) {
                Logger.getLogger(GeometricIndex.class.getName()).log(Level.SEVERE, null, ex);
              }
              // Now quit the thread.
              return;
            }
          }
        } catch (InterruptedException ex) {
          Logger.getLogger(GeometricIndex.class.getName()).log(Level.SEVERE, null, ex);
        }
      }
    };

    mergeThread.start();

    return mergeThread;
  }
*/
  // only one thread can perform a merge at any time.
  // this should stop duplicate merges
  private void mergeIndexShards(int binSize) throws IOException {

    Bin mergeBin = null;
    synchronized (partsLock) {
      mergeBin = geometricParts.findMergeCandidates(binSize,true);
    }

    if (mergeBin == null) {
      //logger.info("Nothing to merge, returning.");
      return;
    } else {
    doMerge(mergeBin);
    }
     
  }

  //wrapper that auto-creates the location
  private void doMerge(Bin mergeBin ) throws IOException {
	// make a new index shard
	  doMerge(mergeBin , getNextIndexShardFolder(mergeBin.size + 1) );
  }
  
  //merges and places the merged index in that location
  private void doMerge(Bin mergeBin , File indexShard) throws IOException{
	  // otherwise there's something to merge
      logger.info("Performing merge!");

      
      // merge the shards
      MergeSequentialIndexShards merger = new MergeSequentialIndexShards(mergeBin.binPaths, indexShard.getAbsolutePath());

      try {
        merger.run(mergeMode);
      } catch (Exception ex) {
        // I just want to see these errors for now...
        Logger.getLogger(GeometricIndex.class.getName()).log(Level.SEVERE, null, ex);
      }

      // should make sure that these two are uninteruppable
      synchronized (partsLock) {
        geometricParts.add(mergeBin.size + 1, indexShard.getAbsolutePath());
        geometricParts.removeShards(mergeBin);
      } // now can delete these folders...

      for (String file : mergeBin.binPaths) {
        Utility.deleteDirectory(new File(file));
      } // find the next merge candidate

      logger.info("Done merging.");
  }
  
  private File getNextIndexShardFolder(int size) {
    File indexFolder = new File(shardDirectory + File.separator + "galagoindex.shard." + indexBlockCount + "." + size);
    indexFolder.mkdirs();
    indexBlockCount++;

    return indexFolder;
  }

  private void resetCurrentMemoryIndex() {
    Parameters parameters = new Parameters();
    parameters.set("firstDocumentId", Long.toString(globalDocumentCount));
    currentMemoryIndex = new MemoryIndex(parameters);
  }

  // Sub - Classes 
  private class Bin {

    int size;
    ArrayList<String> binPaths = new ArrayList();

    public Bin(int size) {
      this.size = size;
    }
    
    public void add(Bin b){
    	binPaths.addAll(b.binPaths);
    }
  }

  private class GeometricPartitions {

    int radix;
    TreeMap<Integer, Bin> radixBins = new TreeMap();

    public GeometricPartitions(int radix) {
      this.radix = radix;
    }

    private Bin get(int size){
    	return radixBins.get(new Integer(size));
    }
    
    private void add(int size, String path) {
      if (!radixBins.containsKey(size)) {
        radixBins.put(size, new Bin(size));
      }
      radixBins.get(size).binPaths.add(path);
    }

    // Specifically returns a new Bin with the set of file paths
    // this means that computation can continue as required.
    //
    // If cascade is true, then this will also add larger bin sizes
    // if merging the current bin size will cause the next one to
    // reach radix.
    private Bin findMergeCandidates(int size, boolean cascade) {
      Bin candidate;
      Bin result = new Bin(0);
      for(int i = size;i<=getMaxSize();i++) {
	      candidate = radixBins.get(i);
	      if (candidate.binPaths.size() + ( (size == i) ?  0 : 1) >= radix) {
	    	logger.info("Adding Merge Candidate of size: " + i);
	    	result.size=i;
	        result.add(candidate);
	      }
	      else 
	    	  break;
	      if(!cascade)
	    	  break;
      }
      
      if(result.binPaths.size()>0)
    	  return result;
      return null;
    }
    
    private Bin getAllShards() {
    	Bin result = new Bin(0);
    	for(Integer i : radixBins.keySet()) {
    		if(i.intValue()>result.size)
    			result.size = i.intValue();
    		result.add(radixBins.get(i));
        }
    	return result;
    }

    // only remove merged shards
    private void removeShards(Bin merged) {
      //search all bins and remove.
      for(Integer i : radixBins.keySet()) {
      	radixBins.get(i).binPaths.removeAll(merged.binPaths);
      }
    }

    private int getMaxSize() {
      int max = 0;
      for (int i : radixBins.keySet()) {
        if (i > max) {
          max = i;
        }
      }
      return max;
    }
  }
}
