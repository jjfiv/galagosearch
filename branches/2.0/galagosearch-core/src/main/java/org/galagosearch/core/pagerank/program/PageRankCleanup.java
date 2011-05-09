// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.pagerank.program;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.pagerank.close.PREntryReader;
import org.galagosearch.core.pagerank.close.PageRankWriter;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.execution.Stage;
import org.galagosearch.tupleflow.execution.Step;

/**
 * Adapted from Stanford's pagerank code (Summer 2010)
 *
 * Job Plan:
 *  1: extract data from pagerank temp folder
 *  2: write data to index folder in pagerank file
 *
 *
 * @author sjh, schiu
 */
public class PageRankCleanup {

  private String indexPath;
  private String pagerankFolder;


  public Stage getWritePREntriesStage() {
    Stage stage = new Stage("outputWriter");

    Parameters params = new Parameters();
    params.add("entries", this.pagerankFolder + File.separator + "entries.pr");
    params.add("filename", this.indexPath + File.separator + "pageranks");

    stage.add(new Step(PREntryReader.class, params));
    stage.add(new Step(PageRankWriter.class, params));

    return stage;
  }

  public Job makeJob(Parameters p) throws IOException {

    Job job = new Job();
    this.indexPath = new File(p.get("indexPath")).getAbsolutePath(); // fail if no path.
    this.pagerankFolder = new File(p.get("pagerankTemp")).getAbsolutePath(); // fail if no path.
    ArrayList<String> inputPaths = new ArrayList();
    List<Value> vs = p.list("inputPaths");
    for (Value v : vs) {
      inputPaths.add(v.toString());
    }

    // ensure the index folder is an index
    StructuredIndex i = new StructuredIndex(indexPath);
    i.close();

    job.add(getWritePREntriesStage());

    return job;
  }
}
