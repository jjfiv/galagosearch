// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.galagosearch.core.index.PositionIndexWriter;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.ngram.DummyDocumentNumberer;
import org.galagosearch.core.index.ExtractIndexDocumentNumbers;
import org.galagosearch.core.ngram.ExtractLocations;
import org.galagosearch.core.ngram.FilterNgrams;
import org.galagosearch.core.ngram.NgramFeatureThresholder;
import org.galagosearch.core.ngram.NgramFeatureWriter;
import org.galagosearch.core.ngram.NgramFeaturer;
import org.galagosearch.core.ngram.NgramProducer;
import org.galagosearch.core.ngram.NgramToNumberWordPosition;
import org.galagosearch.core.ngram.NumberWordPositionThresholder;
import org.galagosearch.core.types.NgramFeature;
import org.galagosearch.core.parse.DocumentSource;
import org.galagosearch.core.parse.Porter2Stemmer;
import org.galagosearch.core.parse.TagTokenizer;
import org.galagosearch.core.parse.UniversalParser;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.core.types.NumberWordPosition;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ConnectionAssignmentType;
import org.galagosearch.tupleflow.execution.ConnectionPointType;
import org.galagosearch.tupleflow.execution.InputStep;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.execution.OutputStep;
import org.galagosearch.tupleflow.execution.Stage;
import org.galagosearch.tupleflow.execution.StageConnectionPoint;
import org.galagosearch.tupleflow.execution.Step;

/**
 *
 * Space efficient algorithm for ngram indexing
 *  - Extra Time: uses two passes through the corpus
 *
 * @author sjh
 */
public class BuildNgramIndexSE {
  String indexPath;
  String filterTempDir;
  boolean stemming;
  int n;
  int threshold;

  public BuildNgramIndexSE() {
    this.stemming = false;
    this.n = 2;
    this.threshold = 2;
  }

  public BuildNgramIndexSE(String indexPath) {
    this.indexPath = indexPath;
    this.stemming = true;
    this.n = 2;
    this.threshold = 2;
  }

  public Stage getSplitStage(ArrayList<String> inputs) throws IOException {
    Stage stage = new Stage("inputSplit");
    stage.add(new StageConnectionPoint(ConnectionPointType.Output, "splits",
        new DocumentSplit.FileIdOrder()));

    Parameters p = new Parameters();
    for (String input : inputs) {
      File inputFile = new File(input);

      if (inputFile.isFile()) {
        p.add("filename", inputFile.getAbsolutePath());
      } else if (inputFile.isDirectory()) {
        p.add("directory", inputFile.getAbsolutePath());
      } else {
        throw new IOException("Couldn't find file/directory: " + input);
      }
    }

    stage.add(new Step(DocumentSource.class, p));
    stage.add(Utility.getSorter(new DocumentSplit.FileIdOrder()));
    stage.add(new OutputStep("splits"));
    return stage;
  }

  public Stage getParseFilterStage(){
    // reads through the corpus
    Stage stage = new Stage("parseFilter");

    stage.add(new StageConnectionPoint(
        ConnectionPointType.Input,
        "splits", new DocumentSplit.FileIdOrder()));
    stage.add(new StageConnectionPoint(
        ConnectionPointType.Output,
        "filterData", new NgramFeature.FeatureOrder()));

    stage.add(new InputStep("splits"));
    stage.add(new Step(UniversalParser.class));
    stage.add(new Step(TagTokenizer.class));
    if(stemming){
      stage.add(new Step(Porter2Stemmer.class));
    }

    stage.add(new Step(DummyDocumentNumberer.class));

    Parameters p2 = new Parameters();
    p2.add("n", Integer.toString(n));
    stage.add(new Step(NgramProducer.class, p2));

    stage.add(new Step(NgramFeaturer.class));
    stage.add( Utility.getSorter( new NgramFeature.FeatureOrder()));
        
    stage.add(new OutputStep("filterData"));

    return stage;
  }
  
  public Stage getReduceFilterStage(){
    Stage stage = new Stage("reduceFilter");
    stage.add(new StageConnectionPoint(
        ConnectionPointType.Input,
        "filterData", new NgramFeature.FeatureOrder()));
    stage.add(new StageConnectionPoint(
        ConnectionPointType.Output,
        "symbolicFilter", new NgramFeature.FileFilePositionOrder()));
    
    stage.add(new InputStep("filterData"));
    
    Parameters p = new Parameters();
    p.add("threshold", Integer.toString(threshold));
    stage.add(new Step(NgramFeatureThresholder.class, p));

    stage.add(Utility.getSorter(new NgramFeature.FileFilePositionOrder()));

    stage.add(new Step(ExtractLocations.class));

    //stage.add(new OutputStep("filter"));
    Parameters p2 = new Parameters();
    p2.add("filterFolder", filterTempDir);
    stage.add(new Step(NgramFeatureWriter.class, p2));
    
    return stage;
  }

  
  public Stage getParsePostingsStage(){
    // reads through the corpus
    Stage stage = new Stage("parsePostings");

    stage.add(new StageConnectionPoint(
        ConnectionPointType.Input,
        "splits", new DocumentSplit.FileIdOrder()));
    stage.add(new StageConnectionPoint(
        ConnectionPointType.Input,
        "symbolicFilter", new NgramFeature.FileFilePositionOrder()));
    stage.add(new StageConnectionPoint(
        ConnectionPointType.Output,
        "postings", new NumberWordPosition.WordDocumentPositionOrder()));

    stage.add(new InputStep("splits"));      
    stage.add(new Step(UniversalParser.class));
    stage.add(new Step(TagTokenizer.class));
    if(stemming){
      stage.add(new Step(Porter2Stemmer.class));
    }

    Parameters p = new Parameters();
    p.add("indexPath", indexPath);
    stage.add(new Step(ExtractIndexDocumentNumbers.class, p));
 
    
    Parameters p2 = new Parameters();
    p2.add("n", Integer.toString(n));
    stage.add(new Step(NgramProducer.class, p2));

    Parameters p3 = new Parameters();
    p3.add("filterFolder", filterTempDir);
    stage.add(new Step(FilterNgrams.class, p3));
    
    stage.add(new Step(NgramToNumberWordPosition.class));

    stage.add( Utility.getSorter( new NumberWordPosition.WordDocumentPositionOrder()));

    stage.add(new OutputStep("postings"));
    return stage;
  }

  
  public Stage getWritePostingsStage(String stageName, String inputName, String indexName) {
    Stage stage = new Stage(stageName);

    stage.add(new StageConnectionPoint(
        ConnectionPointType.Input, inputName,
        new NumberWordPosition.WordDocumentPositionOrder()));

    stage.add(new InputStep(inputName));

    Parameters p = new Parameters();
    p.add("threshold", Integer.toString(threshold));
    stage.add(new Step(NumberWordPositionThresholder.class, p));

    Parameters p2 = new Parameters();
    p2.add("filename", indexPath + File.separator + "parts" + File.separator + indexName);
    stage.add(new Step(PositionIndexWriter.class, p2));
    return stage;
  }

  public Job getIndexJob(Parameters p) throws IOException {

    Job job = new Job();
    this.stemming = p.get("stemming", false);
    this.n = (int) p.get("n", 2);
    this.threshold = (int) p.get("threshold", 2);
    this.indexPath = new File(p.get("indexPath")).getAbsolutePath(); // fail if no path.
    this.filterTempDir = p.get("galagoTemp") + File.separator + "filterData";
    this.filterTempDir = new File(this.filterTempDir).getAbsolutePath();

    ArrayList<String> inputPaths = new ArrayList();
    List<Value> vs = p.list("inputPaths");
    for(Value v : vs){
      inputPaths.add(v.toString());
    }

    // we intend to add to the index;
    // so verify that the index submitted is a valid index
    try{
      StructuredIndex i = new StructuredIndex(indexPath);
    } catch (Exception e){
      throw new IOException("Index " + indexPath + "is not a valid index\n" + e.toString());
    }

    // Next make sure there's no data present in the filter tempDir
    File f = new File(this.filterTempDir);
    if(f.isDirectory())
      Utility.deleteDirectory(f);
    if(f.isFile())
      f.delete();
    // now recreate as a directory
    f.mkdirs();

    // create a new index file name
    String indexName = n + "-grams-" + threshold + "-count" ;
    if(stemming)
      indexName += "-stemmed";

    job.add(getSplitStage(inputPaths));
    job.add(getParseFilterStage());
    job.add(getReduceFilterStage());
    job.add(getParsePostingsStage());

    job.add(getWritePostingsStage("writePostings", "postings", indexName));

    job.connect("inputSplit", "parseFilter", ConnectionAssignmentType.Each);       
    job.connect("inputSplit", "parsePostings", ConnectionAssignmentType.Each);       

    job.connect("parseFilter", "reduceFilter", ConnectionAssignmentType.Each);
    job.connect("reduceFilter", "parsePostings", ConnectionAssignmentType.Each);

    job.connect("parsePostings", "writePostings", ConnectionAssignmentType.Combined);

    return job;
  }
}
