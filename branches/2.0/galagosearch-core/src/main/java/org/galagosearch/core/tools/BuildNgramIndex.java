// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.galagosearch.core.index.PositionIndexWriter;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.index.ExtractIndexDocumentNumbers;
import org.galagosearch.core.index.ngram.NgramProducer;
import org.galagosearch.core.index.ngram.NgramToNumberWordPosition;
import org.galagosearch.core.index.ngram.NumberWordPositionThresholder;
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
 * Time efficient algorithm for ngram indexing
 *  - uses more temporary space
 *  - estimate the space required as (n*|C|)
 *
 * @author sjh
 */
public class BuildNgramIndex {
  String indexPath;
  boolean stemming;
  int n;
  int threshold;

  public BuildNgramIndex() {
    this.stemming = false;
    this.n = 2;
    this.threshold = 2;
  }

  public BuildNgramIndex(String indexPath) {
    this.indexPath = indexPath;
    this.stemming = true;
    this.n = 2;
    this.threshold = 2;
  }

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

    String indexName = n + "-grams-" + threshold + "-count" ;
    if(stemming)
      indexName += "-stemmed";
    
    job.add(getSplitStage(inputPaths));
    job.add(getParsePostingsStage());
    job.add(getWritePostingsStage("writePostings", "postings", indexName));

    job.connect("inputSplit", "parsePostings", ConnectionAssignmentType.Each);       
    job.connect("parsePostings", "writePostings", ConnectionAssignmentType.Combined);

    return job;
  }
}
