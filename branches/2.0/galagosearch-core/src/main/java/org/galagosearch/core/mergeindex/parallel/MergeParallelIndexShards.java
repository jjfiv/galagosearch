// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.mergeindex.parallel;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ConnectionAssignmentType;
import org.galagosearch.tupleflow.execution.ConnectionPointType;
import org.galagosearch.tupleflow.execution.ErrorStore;
import org.galagosearch.tupleflow.execution.InputStep;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.execution.JobExecutor;
import org.galagosearch.tupleflow.execution.OutputStep;
import org.galagosearch.tupleflow.execution.Stage;
import org.galagosearch.tupleflow.execution.StageConnectionPoint;
import org.galagosearch.tupleflow.execution.Step;

/**
 * Index Merger 
 * 
 * Unlike the sequential mergers this merger 
 * does not assume that documents are uniquely numbered
 * Rather we assume that we have to provide a new unique 
 * document number for each document in each index
 * 
 * @author sjh
 */
public class MergeParallelIndexShards {

  ArrayList<String> diskShardPaths = new ArrayList();
  String outputIndexPath;
  File tempFolder;
  boolean stemming = false;

  
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

  private Stage getDocumentNumberMappingStage(){
    Stage stage = new Stage("mapping");
    
    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
        "indexes", new DocumentSplit.FileIdOrder()));
    stage.add(new StageConnectionPoint(ConnectionPointType.Output,
        "mapping", new DocumentSplit.FileIdOrder()));

    stage.add(new InputStep("indexes"));
    stage.add(new Step(CreateNumberMapping.class));

    Parameters p = new Parameters();
    p.add("filename", tempFolder.getAbsolutePath() + File.separator + "documentMappingData");
    stage.add(new Step(NumberMappingWriter.class, p));
    
    stage.add(new OutputStep("mapping"));
    
    return stage;
  }

  private Stage getMergeManifestStage(){
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
    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            "mapping", new DocumentSplit.FileIdOrder()));


    stage.add(new InputStep("indexes"));
    stage.add(new Step(ExtractLengths.class));
    Parameters p = new Parameters();
    p.add("stream", "mapping");
    stage.add(new Step(NumberedDocumentDataIteratorMerger.class, p));

    Parameters p2 = new Parameters();
    p2.add("filename", outputIndexPath + File.separator + "documentLengths");
    stage.add(new Step(DocumentLengthsWriter.class, p2));

    return stage;
  }

  private Stage getMergeNamesStage() {
    Stage stage = new Stage("mergeNames");
    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            "indexes", new DocumentSplit.FileIdOrder()));
    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            "mapping", new DocumentSplit.FileIdOrder()));

    stage.add(new InputStep("indexes"));
    stage.add(new Step(ExtractNames.class));

    Parameters p = new Parameters();
    p.add("stream", "mapping");
    stage.add(new Step(NumberedDocumentDataIteratorMerger.class, p));
    
    Parameters p2 = new Parameters();
    p2.add("filename", outputIndexPath + File.separator + "documentNames");
    stage.add(new Step(DocumentNameWriter.class, p2));

    return stage;
  }


  private Stage getMergePartStage(String part) {
    Stage stage = new Stage("mergePart-" + part);
    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            "indexes", new DocumentSplit.FileIdOrder()));
    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            "mapping", new DocumentSplit.FileIdOrder()));

    stage.add(new InputStep("indexes"));
    Parameters p = new Parameters();
    p.add("part", part);
    p.add("filename", outputIndexPath + File.separator + "parts" + File.separator + part);
    stage.add(new Step(ExtractPartIterator.class, p));

    Parameters p2 = new Parameters();
    p2.add("stream", "mapping");
    stage.add(new Step(ExtentIteratorMerger.class, p2));
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


  public Job getJob(Parameters p) throws IOException {
    Job job = new Job();

    this.outputIndexPath = new File(p.get("outputIndex")).getAbsolutePath();
    List<Value> inputs = p.list("inputIndexes");
    for(Value in : inputs){
      this.diskShardPaths.add(in.toString());
    }

    for (String path : this.diskShardPaths) {
      // check for valid indexes
      StructuredIndex i = new StructuredIndex(path);
      if (i.containsPart("stemmedPostings")) {
        stemming = true;
      }
    }
    // ensure output folders exist
    new File(outputIndexPath + File.separator + "parts").mkdirs();
    System.err.println("made directory");
    //this.tempFolder = new File(p.get("galagoTemp", ""));
    this.tempFolder = new File(p.get("galagoTemp"));

    job.add(getIndexPathStage());
    job.add(getDocumentNumberMappingStage());
    job.add(getMergeManifestStage());
    job.add(getMergeLengthsStage());
    job.add(getMergeNamesStage());
    job.add(getMergePartStage("extents"));
    job.add(getMergePartStage("postings"));
    if (stemming) {
      job.add(getMergePartStage("stemmedPostings"));
    }

    job.connect("input", "mapping", ConnectionAssignmentType.Combined);
    job.connect("input", "mergeManifest", ConnectionAssignmentType.Combined);
    job.connect("input", "mergeLengths", ConnectionAssignmentType.Combined);
    job.connect("mapping", "mergeLengths", ConnectionAssignmentType.Combined);
    job.connect("input", "mergeNames", ConnectionAssignmentType.Combined);
    job.connect("mapping", "mergeNames", ConnectionAssignmentType.Combined);
    job.connect("input", "mergePart-extents", ConnectionAssignmentType.Combined);
    job.connect("mapping", "mergePart-extents", ConnectionAssignmentType.Combined);
    job.connect("input", "mergePart-postings", ConnectionAssignmentType.Combined);
    job.connect("mapping", "mergePart-postings", ConnectionAssignmentType.Combined);
    if (stemming) {
      job.connect("input", "mergePart-stemmedPostings", ConnectionAssignmentType.Combined);
      job.connect("mapping", "mergePart-stemmedPostings", ConnectionAssignmentType.Combined);
    }

    return job;
  }

}
