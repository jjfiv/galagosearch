// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.geometric;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.galagosearch.core.index.corpus.DocumentReader;

import org.galagosearch.core.index.mem.FlushToDisk;
import org.galagosearch.core.index.mem.MemoryIndex;
import org.galagosearch.core.index.mem.MemoryParameters;
import org.galagosearch.core.index.mem.MemoryRetrieval;
import org.galagosearch.core.mergeindex.sequential.MergeSequentialIndexShards;
import org.galagosearch.core.parse.NumberedDocument;
import org.galagosearch.core.retrieval.MultiRetrieval;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.structured.DocumentOrderedFeatureFactory;
import org.galagosearch.core.retrieval.structured.StructuredRetrieval;
import org.galagosearch.core.store.DocumentIndexStore;
import org.galagosearch.core.store.DocumentStore;
import org.galagosearch.core.store.NullStore;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;


/*
 *  author: sjh, schiu
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
public class GeometricIndex extends MultiRetrieval implements Processor<NumberedDocument> {

  public long globalDocumentCount;
  private Logger logger = Logger.getLogger(GeometricIndex.class.toString());
  private String shardDirectory;
  private int indexBlockSize; // measured in documents
  private int indexBlockCount;
  private boolean stemming;
  private boolean makecorpus;
  private String mergeMode;
  private MemoryIndex currentMemoryIndex;
  private GeometricPartitions geometricParts;
  private Parameters retrievalParameters = new Parameters();
  private Parameters statistics;
  private MemoryRetrieval currentMemoryRetrieval;
  // a geometric needs to maintain it's own document store
  // as each flush or merge should alter the store.
  private MergeingDocumentStore docStore = new MergeingDocumentStore();
  // checkpoint data
  private CheckPointHandler checkpointer;
  private String lastAddedDocumentIdentifier = "";
  private int lastAddedDocumentNumber = -1;

  // need a constructor from a checkpoint object!
  public GeometricIndex(TupleFlowParameters parameters) throws Exception {
    this(parameters, new CheckPointHandler());
  }
  
  public GeometricIndex(TupleFlowParameters parameters, CheckPointHandler checkpointer) throws Exception {

    //note: need to find some way to pass in retrieval parameters
    //stopgap justfor testing
    super(new HashMap<String, Collection<Retrieval>>(), new Parameters());


    shardDirectory = parameters.getXML().get("shardDirectory");
    stemming = (boolean) parameters.getXML().get("stemming", true);
    makecorpus = (boolean) parameters.getXML().get("makecorpus", false);
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
    retrievalParameters.add("retrievalGroup", "all");

    this.checkpointer = checkpointer;
    this.checkpointer.setDirectory( shardDirectory );
    if(parameters.getXML().get("resumable", false)){
      restoreToCheckpoint( checkpointer.getRestore() );
    }
    
    resetCurrentMemoryIndex();
    updateRetrieval();
  }

  public void process(NumberedDocument doc) throws IOException {
    currentMemoryIndex.process(doc);
    globalDocumentCount++; // now one higher than the document just processed
    if (globalDocumentCount % indexBlockSize == 0) {
      lastAddedDocumentIdentifier = doc.identifier;
      lastAddedDocumentNumber = doc.number;

      flushCurrentIndexBlock();
      maintainMergeLocal();
    }
  }

  public void close() throws IOException {
    // this will ensure that all data is on disk
    flushCurrentIndexBlock();

    logger.info("Performing final merge");

    try {
      doMerge(geometricParts.getAllShards(), new File(shardDirectory + File.separator + "final"));
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

    final GeometricIndex g = this;
    final MemoryIndex flushingMemoryIndex = currentMemoryIndex;
    final File shardFolder = getNextIndexShardFolder(1);

    // reset the current index for the next document
    resetCurrentMemoryIndex();

    try {
      // first flush the index to disk
      (new FlushToDisk()).flushMemoryIndex(flushingMemoryIndex, shardFolder.getAbsolutePath(), false);

      // indicate that the flushing part of this thread is done
      synchronized (geometricParts) {
        // add flushed index to the set of bins -- needs to be a synconeous action
        geometricParts.add(1, shardFolder.getAbsolutePath());
        updateRetrieval();
        flushingMemoryIndex.close();
      }

    } catch (IOException e) {
      logger.severe(e.toString());
    }
  }

  private void maintainMergeLocal() {
    logger.info("Maintaining Merge Local");
    int i = 1;
    // while (i <= geometricParts.getMaxSize()) {
    try {
      mergeIndexShards(i);
    } catch (IOException ex) {
      Logger.getLogger(GeometricIndex.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  // only one thread can perform a merge at any time.
  // this should stop duplicate merges
  private void mergeIndexShards(int binSize) throws IOException {

    Bin mergeBin = null;
    synchronized (geometricParts) {
      mergeBin = geometricParts.findMergeCandidates(binSize, true);
    }

    if (mergeBin == null) {
      //logger.info("Nothing to merge, returning.");
      return;
    } else {
      doMerge(mergeBin);
    }

  }

  //wrapper that auto-creates the location
  private void doMerge(Bin mergeBin) throws IOException {
    // make a new index shard
    doMerge(mergeBin, getNextIndexShardFolder(mergeBin.size + 1));
  }

  //merges and places the merged index in that location
  private void doMerge(Bin mergeBin, File indexShard) throws IOException {
    // otherwise there's something to merge
    logger.info("Performing merge!");


    // merge the shards
    MergeSequentialIndexShards merger = new MergeSequentialIndexShards(mergeBin.getBinPaths(), indexShard.getAbsolutePath());

    try {
      merger.run(mergeMode);
    } catch (Exception ex) {
      // I just want to see these errors for now...
      Logger.getLogger(GeometricIndex.class.getName()).log(Level.SEVERE, null, ex);
    }

    // should make sure that these two are uninteruppable
    synchronized (geometricParts) {
      geometricParts.add(mergeBin.size + 1, indexShard.getAbsolutePath());
      geometricParts.removeShards(mergeBin);
      updateRetrieval();
    } // now can delete these folders...

    for (String file : mergeBin.getBinPaths()) {
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
    parameters.set("stemming", Boolean.toString(stemming));
    parameters.set("makecorpus", Boolean.toString(makecorpus));
    currentMemoryIndex = new MemoryIndex(parameters);
  }

  /* this function is called after each index flush
   *  and after each index merge operation
   */
  private void updateRetrieval() throws IOException {

    // collect all relevent retrieval objects
    Collection<Retrieval> r = geometricParts.getAllShards().getAllRetrievals();
    currentMemoryRetrieval = new MemoryRetrieval(currentMemoryIndex, retrievalParameters);
    ArrayList<Retrieval> allRetrievals = new ArrayList<Retrieval>(r.size() + 1);
    allRetrievals.addAll(r);
    allRetrievals.add(currentMemoryRetrieval);
    retrievals.put("all", allRetrievals);


    // collect all relevent statisitics (eg: doc counts, term counts)
    ArrayList<Parameters> staticParameters = new ArrayList<Parameters>();
    for (Retrieval ret : r) {
      staticParameters.add(ret.getRetrievalStatistics("all"));
    }

    // merge the statistics together
    statistics = new MemoryParameters((MemoryRetrieval) currentMemoryRetrieval, mergeStats(staticParameters));
    statistics.add("retrievalGroup", "all");

    // maintain the document store (corpus) - if there is one
    if (this.makecorpus) {
      // get all corpora
      // shove into document store
      ArrayList<DocumentReader> readers = new ArrayList<DocumentReader>();
      readers.add(currentMemoryIndex.getDocumentReader());
      for (String path : geometricParts.getAllShards().getBinPaths()) {
        String corpus = path + File.separator + "corpus";
        readers.add(DocumentReader.getInstance(corpus));
      }
      this.docStore.setDocStore(new DocumentIndexStore(readers));
    }

    // write new checkpointing data (only for on disk indexes)
    Parameters checkpoint = createCheckpoint();
    this.checkpointer.saveCheckpoint(checkpoint);

    initRetrieval();
  }

  //MultiRetrieval modifications
  //simplify, since statistics need to be recompiled everytime.
  private void initRetrieval() throws IOException {
    featureFactories = new HashMap();
    featureFactories.put("all", new DocumentOrderedFeatureFactory(getRetrievalStatistics("all")));

  }

  //since we know all the retrievals will be the same, just return any of them
  public Parameters getAvailableParts(String retGroup) throws IOException {
    return currentMemoryRetrieval.getAvailableParts("all");
  }

  public Parameters getRetrievalStatistics(String retGroup) throws IOException {

    return statistics;

  }

  // should contain parameters that can be used to restart the geometric index
  // should also contain the last document identifier that was written to disk
  // (copied from the last flush)
  private Parameters createCheckpoint() {
    Parameters checkpoint = new Parameters();
    checkpoint.add("lastDoc/identifier", this.lastAddedDocumentIdentifier);
    checkpoint.add("lastDoc/number", Integer.toString(this.lastAddedDocumentNumber));
    checkpoint.add("indexBlockCount",Integer.toString(this.indexBlockCount));
    for (Bin b : this.geometricParts.radixBins.values()) {
      for (String path : b.getBinPaths()) {
        checkpoint.add("shards/bin-" + b.size, path);
      }
    }
    return checkpoint;
  }

  // should only be called at startup
  private void restoreToCheckpoint(Parameters checkpoint) {
    this.lastAddedDocumentIdentifier = checkpoint.get("lastDoc/identifier");
    this.lastAddedDocumentNumber = Integer.parseInt(checkpoint.get("lastDoc/number"));
    this.indexBlockCount = Integer.parseInt(checkpoint.get("indexBlockCount"));
    Value shards = checkpoint.list("shards").get(0);
    for(String size : shards.listKeys()){
      int s = Integer.parseInt(size.split("-")[1]);
      for(String path : shards.stringList( size )){
        this.geometricParts.add(s, path);
      }
    }
  }

  // Sub - Classes
  private class Bin {

    int size;
    private TreeMap<String, Retrieval> binPaths = new TreeMap<String, Retrieval>();

    public Bin(int size) {
      this.size = size;
    }

    public void add(Bin b) {
      binPaths.putAll(b.binPaths);
    }

    public void add(String path) {
      try {
        binPaths.put(path, new StructuredRetrieval(path, retrievalParameters));
      } catch (Exception e) {
        logger.info("Index " + path + " cannot be opened.");
      }

    }

    private void removeAll(Bin b) {
      for (String key : b.getBinPaths()) {
        binPaths.remove(key);
      }
    }

    private int size() {
      return binPaths.size();
    }

    private Collection<String> getBinPaths() {
      return binPaths.keySet();
    }

    public Collection<Retrieval> getAllRetrievals() {
      return binPaths.values();
    }
  }

  private class GeometricPartitions {

    int radix;
    TreeMap<Integer, Bin> radixBins = new TreeMap();

    public GeometricPartitions(int radix) {
      this.radix = radix;
    }

    private Bin get(int size) {
      return radixBins.get(new Integer(size));
    }

    private void add(int size, String path) {
      if (!radixBins.containsKey(size)) {
        radixBins.put(size, new Bin(size));
      }
      radixBins.get(size).add(path);
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
      for (int i = size; i <= getMaxSize(); i++) {
        candidate = radixBins.get(i);
        if (candidate.size() + ((size == i) ? 0 : 1) >= radix) {
          logger.info("Adding Merge Candidate of size: " + i);
          result.size = i;
          result.add(candidate);
        } else {
          break;
        }
        if (!cascade) {
          break;
        }
      }

      if (result.size() > 0) {
        return result;
      }
      return null;
    }

    private Bin getAllShards() {
      Bin result = new Bin(0);
      for (Integer i : radixBins.keySet()) {
        if (i.intValue() > result.size) {
          result.size = i.intValue();
        }
        result.add(radixBins.get(i));
      }
      return result;
    }

    // only remove merged shards
    private void removeShards(Bin merged) {
      //search all bins and remove.
      for (Integer i : radixBins.keySet()) {
        radixBins.get(i).removeAll(merged);
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
