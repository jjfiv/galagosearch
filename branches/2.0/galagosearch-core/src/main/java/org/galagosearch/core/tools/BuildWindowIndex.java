// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.galagosearch.core.index.ExtentIndexWriter;
import org.galagosearch.core.index.PositionIndexWriter;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.index.ExtractIndexDocumentNumbers;
import org.galagosearch.core.window.WindowProducer;
import org.galagosearch.core.parse.DocumentSource;
import org.galagosearch.core.parse.Porter2Stemmer;
import org.galagosearch.core.parse.TagTokenizer;
import org.galagosearch.core.parse.UniversalParser;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.core.types.NumberWordPosition;
import org.galagosearch.core.types.NumberedExtent;
import org.galagosearch.core.window.NumberedExtentThresholder;
import org.galagosearch.core.window.WindowToNumberedExtent;
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
 * Time efficient algorithm for ngram indexing
 *  - uses more temporary space
 *  - estimate the space required as (n*|C|)
 *
 * @author sjh
 */
public class BuildWindowIndex {
  String indexPath;
  boolean stemming;
  int n;
  int width;
  boolean ordered;
  int threshold;
  boolean threshdf;

  public Stage getSplitStage(ArrayList<String> inputs) throws IOException {
    Stage stage = new Stage("inputSplit");
    stage.add(new StageConnectionPoint(ConnectionPointType.Output, "splits",
        new DocumentSplit.FileNameStartKeyOrder()));

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
    stage.add(Utility.getSorter(new DocumentSplit.FileNameStartKeyOrder()));
    stage.add(new OutputStep("splits"));
    return stage;
  }

  public Stage getParsePostingsStage(){
    // reads through the corpus
    Stage stage = new Stage("parsePostings");

    stage.add(new StageConnectionPoint(
        ConnectionPointType.Input,
        "splits", new DocumentSplit.FileNameStartKeyOrder()));
    stage.add(new StageConnectionPoint(
        ConnectionPointType.Output,
        "windows", new NumberedExtent.ExtentNameNumberBeginOrder()));

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
    p2.add("width", Integer.toString(width));
    p2.add("ordered", Boolean.toString(ordered));
    stage.add(new Step(WindowProducer.class, p2));

    stage.add(new Step(WindowToNumberedExtent.class));

    stage.add( Utility.getSorter( new NumberedExtent.ExtentNameNumberBeginOrder()));

    stage.add(new OutputStep("windows"));
    return stage;
  }

  public Stage getWritePostingsStage(String stageName, String inputName, String indexName) {
    Stage stage = new Stage(stageName);

    stage.add(new StageConnectionPoint(
        ConnectionPointType.Input, inputName,
        new NumberedExtent.ExtentNameNumberBeginOrder()));

    stage.add(new InputStep(inputName));

    Parameters p = new Parameters();
    p.add("threshold", Integer.toString(threshold));
    p.add("threshdf", Boolean.toString(threshdf));
    stage.add(new Step(NumberedExtentThresholder.class, p));

    Parameters p2 = new Parameters();
    p2.add("filename", indexPath + File.separator + indexName);
    stage.add(new Step(ExtentIndexWriter.class, p2));
    return stage;
  }

  public Job getIndexJob(Parameters p) throws IOException {

    Job job = new Job();
    this.stemming = p.get("stemming", false);
    this.n = (int) p.get("n", 2);
    this.width = (int) p.get("width", 1);
    this.ordered = p.get("ordered", true);
    this.threshold = (int) p.get("threshold", 2);
    this.threshdf = p.get("usedocfreq", false);

    this.indexPath = new File(p.get("indexPath")).getAbsolutePath(); // fail if no path.

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

    String indexName;
    if(ordered)
      indexName = "n" + n + "-w" + width + "-ordered" + "-h" + threshold ;
    else
      indexName = "n" + n + "-w" + width + "-unordered" + "-h" + threshold;

    if(threshdf)
      indexName += "-docfreq";

    if(stemming)
      indexName += "-stemmed";


    job.add(getSplitStage(inputPaths));
    job.add(getParsePostingsStage());
    job.add(getWritePostingsStage("writePostings", "windows", indexName));

    job.connect("inputSplit", "parsePostings", ConnectionAssignmentType.Each);       
    job.connect("parsePostings", "writePostings", ConnectionAssignmentType.Combined);

    return job;
  }
}
