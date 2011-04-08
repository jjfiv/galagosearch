// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.mergeindex.sequential;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import org.galagosearch.core.index.DocumentLengthsWriter;
import org.galagosearch.core.index.DocumentNameWriter;
import org.galagosearch.core.index.ExtentIndexWriter;

import org.galagosearch.core.index.ManifestWriter;
import org.galagosearch.core.index.PositionIndexWriter;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.mergeindex.AssertNumberWordPositionOrder;
import org.galagosearch.core.mergeindex.AssertNumberedExtentOrder;
import org.galagosearch.core.mergeindex.IndexPathNumberer;
import org.galagosearch.core.mergeindex.KeyExtentToNumberWordPosition;
import org.galagosearch.core.mergeindex.KeyExtentToNumberedExtent;
import org.galagosearch.core.mergeindex.ManifestMerger;
import org.galagosearch.core.mergeindex.extractor.ExtractLengths;
import org.galagosearch.core.mergeindex.extractor.ExtractManifest;
import org.galagosearch.core.mergeindex.extractor.ExtractNames;
import org.galagosearch.core.mergeindex.extractor.ExtractPartIterator;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Step;
import org.galagosearch.tupleflow.execution.ConnectionAssignmentType;
import org.galagosearch.tupleflow.execution.ConnectionPointType;
import org.galagosearch.tupleflow.execution.ErrorStore;
import org.galagosearch.tupleflow.execution.InputStep;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.execution.JobExecutor;
import org.galagosearch.tupleflow.execution.OutputStep;
import org.galagosearch.tupleflow.execution.Stage;
import org.galagosearch.tupleflow.execution.StageConnectionPoint;

/**
 *
 *  Creates a merge index job
 *   - Merges two or more indexes (that have been created by the geometric indexer
 *   - The documents in each index to be merged are assumed to be distinct sets
 *   - Furthermore documents are assumed to be numbered uniquely
 *
 * @author sjh
 */
public class MergeSequentialIndexShards {

  Iterable<String> diskShardPaths;
  String outputIndexPath;
  //ArrayList<Index> inputIndexes = new ArrayList();
  boolean stemming = false;

  public MergeSequentialIndexShards(Iterable<String> diskShardPaths, String outputIndexPath) throws IOException {

    this.diskShardPaths = diskShardPaths;
    this.outputIndexPath = outputIndexPath;

    for (String path : this.diskShardPaths) {
      // check for valid indexes
      StructuredIndex i = new StructuredIndex(path);
      i.close();
      System.err.println("inputIndex = " + path);
      if (i.containsPart("stemmedPostings")) {
        stemming = true;
      }
    }

    // ensure the output folders exist
    new File(outputIndexPath + File.separator + "parts").mkdirs();

  }

  public Job getJob() {
    Job job = new Job();

    job.add(getIndexPathStage());
    job.add(getMergeManifestStage());
    job.add(getMergeLengthsStage());
    job.add(getMergeNamesStage());
    job.add(getMergePartStage("extents"));
    job.add(getMergePartStage("postings"));
    if (stemming) {
      job.add(getMergePartStage("stemmedPostings"));
    }

    job.connect("input", "mergeManifest", ConnectionAssignmentType.Combined);
    job.connect("input", "mergeLengths", ConnectionAssignmentType.Combined);
    job.connect("input", "mergeNames", ConnectionAssignmentType.Combined);
    job.connect("input", "mergePart-extents", ConnectionAssignmentType.Combined);
    job.connect("input", "mergePart-postings", ConnectionAssignmentType.Combined);
    if (stemming) {
      job.connect("input", "mergePart-stemmedPostings", ConnectionAssignmentType.Combined);
    }

    return job;
  }

  public void run(String mode) throws Exception {

    Job job = this.getJob();
    ErrorStore store = new ErrorStore();
    Parameters p = new Parameters();
    p.set("mode", mode);
    JobExecutor.runLocally(job, store, p);
    if (store.hasStatements()) {
      System.err.println(store.toString());
    }
  }

  private Stage getIndexPathStage() {
    Stage stage = new Stage("input");

    stage.add(new StageConnectionPoint(ConnectionPointType.Output,
            "indexes", new DocumentSplit.FileIdOrder()));

    Parameters p = new Parameters();
    for (String index : diskShardPaths) {
      File f = new File(index);
      p.add("inputIndex", f.getAbsolutePath());
    }

    stage.add(new Step(IndexPathNumberer.class, p));
    stage.add(new OutputStep("indexes"));

    return stage;
  }

  private Stage getMergeManifestStage() {
    Stage stage = new Stage("mergeManifest");

    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            "indexes", new DocumentSplit.FileIdOrder()));

    stage.add(new InputStep("indexes"));
    stage.add(new Step(ExtractManifest.class));
    stage.add(new Step(ManifestMerger.class));
    Parameters p = new Parameters();
    p.add("filename", outputIndexPath + File.separator + "manifest");
    stage.add(new Step(ManifestWriter.class, p));

    return stage;
  }

  private Stage getMergeLengthsStage() {
    Stage stage = new Stage("mergeLengths");

    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            "indexes", new DocumentSplit.FileIdOrder()));

    stage.add(new InputStep("indexes"));
    stage.add(new Step(ExtractLengths.class));
    stage.add(new Step(NumberedDocumentDataIteratorMerger.class));
    Parameters p = new Parameters();
    p.add("filename", outputIndexPath + File.separator + "documentLengths");
    stage.add(new Step(DocumentLengthsWriter.class, p));

    return stage;
  }

  private Stage getMergeNamesStage() {
    Stage stage = new Stage("mergeNames");
    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            "indexes", new DocumentSplit.FileIdOrder()));

    stage.add(new InputStep("indexes"));
    stage.add(new Step(ExtractNames.class));
    stage.add(new Step(NumberedDocumentDataIteratorMerger.class));
    Parameters p = new Parameters();
    p.add("filename", outputIndexPath + File.separator + "documentNames");
    stage.add(new Step(DocumentNameWriter.class, p));

    return stage;
  }

  private Stage getMergePartStage(String part) {
    Stage stage = new Stage("mergePart-" + part);
    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            "indexes", new DocumentSplit.FileIdOrder()));

    stage.add(new InputStep("indexes"));
    Parameters p = new Parameters();
    p.add("part", part);
    p.add("filename", outputIndexPath + File.separator + "parts" + File.separator + part);
    stage.add(new Step(ExtractPartIterator.class, p));
    stage.add(new Step(ExtentIteratorMerger.class));
    if (part.equals("extents")) {
      stage.add(new Step(KeyExtentToNumberedExtent.class));
      stage.add(new Step(AssertNumberedExtentOrder.class));
      stage.add(new Step(ExtentIndexWriter.class, p));
    } else {
      stage.add(new Step(KeyExtentToNumberWordPosition.class));
      stage.add(new Step(AssertNumberWordPositionOrder.class));
      stage.add(new Step(PositionIndexWriter.class, p));
    }

    return stage;
  }

  // testing function
  // takes a set of index paths and merges them into the output file path
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Needs some arguments: <outputIndex> <inputIndexes>+");
      return;
    }

    String output = args[0];
    File o = new File(output);
    if (o.isFile()) {
      o.delete();
    }
    if (o.isDirectory()) {
      Utility.deleteDirectory(new File(output));
    }
    // now make an empty folder;
    o.mkdirs();


    String[] inputs = Utility.subarray(args, 1);
    ArrayList<String> inputPaths = new ArrayList();
    for(String input : inputs){
      File f = new File(input);
      inputPaths.add(f.getAbsolutePath());
    }

    MergeSequentialIndexShards merger = new MergeSequentialIndexShards(inputPaths, o.getAbsolutePath());
    merger.run("threaded");
    System.err.println("Completed Merge Successfully!");
  }
}
