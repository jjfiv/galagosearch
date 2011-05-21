// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.mem;

import java.io.File;
import java.io.IOException;

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
public class TestQueryMemoryIndex {
  private static String queryFile;

  public TestQueryMemoryIndex() {
  }

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
    p.add("queryFile", queryFile);
    p.add("makecorpus", "true");
    stage.add(new Step(TestMemoryIndexQuerier.class, p));

    return stage;
  }

  public Job getIndexJob(String[] indexInputs) throws IOException {
    Job job = new Job();

    job.add(getSplitStage(indexInputs));
    job.add(getIndexerStage());

    job.connect("inputSplit", "indexer", ConnectionAssignmentType.Combined);

    return job;
  }

  public static void main(String[] args) throws Exception {
    if (args.length <= 0) {
      System.err.println("Needs some parameters -- input documents?");
      return;
    }

    // handle --links and --stemming flags
    String[][] filtered = Utility.filterFlags(args);

    String[] flags = filtered[0];
    String[] nonFlags = filtered[1];
    //String indexDir = nonFlags[0];
    //String[] docs = Utility.subarray(nonFlags, 1);
    String[] docs = filtered[1];

    Parameters p = new Parameters(flags);
    //boolean useLinks = p.get("links", false);
    //boolean stemming = p.get("stemming", true);
    //String indexunit = p.get("indexunit", "");
    int deleteOutput = Integer.parseInt(p.get("deleteOutput", "2"));
    String hash = p.get("distrib", "0");
    String mode = p.get("mode", "local");
    String tempFolderPath = p.get("galagoTemp", "");

    queryFile = p.get("queryFile");

    File tempFolder = Utility.createGalagoTempDir(tempFolderPath);

    TestQueryMemoryIndex build = new TestQueryMemoryIndex();
    Job job = build.getIndexJob(docs);
    ErrorStore store = new ErrorStore();

    try {
      int h = Integer.parseInt(hash);
      if (h > 0) {
        job.properties.put("hashCount", hash);
      }
    } catch (NumberFormatException e) {
      // if it's not a number, then nothing needs to be done for the default
    }

    JobExecutor.runLocally(job, store, p);
    if (store.hasStatements()) {
      System.err.println(store.toString());
    }
  }
}
