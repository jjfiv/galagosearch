// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index.geometric;

import java.io.File;
import java.io.IOException;

import org.galagosearch.core.index.mem.MemoryChecker;
import org.galagosearch.core.parse.DocumentSource;
import org.galagosearch.core.parse.SequentialDocumentNumberer;
import org.galagosearch.core.parse.TagTokenizer;
import org.galagosearch.core.parse.UniversalParser;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.tupleflow.Parameters;
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
 *
 * @author sjh
 */
public class TestQueryGeometricMemoryIndex {

  static String outputIndexDir;
  static int indexBlockSize;
  static int radix;
  static String mergeMode;
  static String queryFile;

  public TestQueryGeometricMemoryIndex() {}

  public Stage getSplitStage(String[] inputs) throws IOException {
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

  public Stage getIndexerStage() {
    Stage stage = new Stage("indexer");

    stage.add(new StageConnectionPoint(
        ConnectionPointType.Input,
        "splits", new DocumentSplit.FileIdOrder()));

    stage.add(new InputStep("splits"));

    stage.add(new Step(UniversalParser.class));
    stage.add(new Step(TagTokenizer.class));
    stage.add(new Step(SequentialDocumentNumberer.class));
    stage.add(new Step(MemoryChecker.class));

    Parameters p = new Parameters();
    p.add("directory", outputIndexDir);
    p.add("indexBlockSize", Integer.toString(indexBlockSize));
    p.add("radix", Integer.toString(radix));
    p.add("mergeMode", mergeMode);
    p.add("queryFile", queryFile);
    stage.add(new Step(TestGeometricIndexQuerier.class, p));

    return stage;
  }


  public Job getIndexJob(String[] indexInputs) throws IOException {
    Job job = new Job();

    job.add(getSplitStage(indexInputs));
    job.add(getIndexerStage());

    job.connect("inputSplit", "indexer", ConnectionAssignmentType.Combined);

    return job;
  }


  public static void main(String[] args) throws Exception{
    if (args.length <= 0) {
      System.err.println("Needs some parameters -- output / input ?");
      return;
    }

    // handle --links and --stemming flags
    String[][] filtered = Utility.filterFlags(args);

    String[] flags = filtered[0];
    String[] nonFlags = filtered[1];
    String indexDir = nonFlags[0];
    String[] docs = Utility.subarray(nonFlags, 1);

    File dir = new File(indexDir);
    dir.mkdir();
    outputIndexDir = dir.getAbsolutePath();
    
    Parameters p = new Parameters(flags);
    
    String hash = p.get("distrib", "0");
    String tempFolderPath = p.get("galagoTemp", "");
    indexBlockSize = (int) p.get("blockSize", 600); // could use 50000
    radix = (int) p.get("radix", 3);
    mergeMode = p.get("mergeMode", "local");

    queryFile = p.get("queryFile");

    File tempFolder = Utility.createGalagoTempDir(tempFolderPath);

    TestQueryGeometricMemoryIndex build = new TestQueryGeometricMemoryIndex();
    Job job = build.getIndexJob(docs);
    ErrorStore store = new ErrorStore();

    try{
        int h = Integer.parseInt(hash);
        if(h > 0)
          job.properties.put("hashCount", hash);
      } catch (NumberFormatException e){
        // if it's not a number, then nothing needs to be done for the default
      }
  
    JobExecutor.runLocally(job, store, p);
    if (store.hasStatements()) {
      System.err.println(store.toString());
    }
  }
}
