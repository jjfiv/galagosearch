// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.pagerank.program;

import java.io.File;
import java.io.IOException;

import org.galagosearch.core.pagerank.init.PREntryWriter;
import org.galagosearch.core.pagerank.iter.PRAllContribAdder;
import org.galagosearch.core.pagerank.iter.PRAllContribCollector;
import org.galagosearch.core.pagerank.iter.PRDeltaCalculator;
import org.galagosearch.core.pagerank.iter.PRMapper;
import org.galagosearch.core.pagerank.iter.PRReader;
import org.galagosearch.core.pagerank.iter.PRReducer;
import org.galagosearch.core.types.PREntry;
import org.galagosearch.tupleflow.Parameters;
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
 * Adapted from Stanford's pagerank code (Summer 2010)
 *
 * Job Plan:
 *  1: extracts pagerank values + links
 *  2: calculates new random jump values
 *  3: distributes remaining value along links
 *  4: re-accumulates value
 *  5: writes new values over old file
 *
 * @author sjh
 */
public class PageRankIteration {
  private String pagerankTemp;
  private double lambda;
  private double convergenceThreshold;

  public Stage getPRMapStage() {
    Stage stage = new Stage("PageRankMap");

    String newPREntries = "newPageRankEntries";

    stage.add(new StageConnectionPoint(ConnectionPointType.Output,
        newPREntries, new PREntry.DocNumOrder()));

    //PRReader should read in the entries and links
    //and construct a document
    Parameters p = new Parameters();
    p.add("entries", pagerankTemp + File.separator + "entries.pr");
    p.add("links", pagerankTemp + File.separator + "links.pr");
    stage.add(new Step(PRReader.class , p));

    //calculates and sums contributions to all pages
    //writes this to a temp file for later
    Parameters p2 = new Parameters();
    p2.add("manifest", pagerankTemp + File.separator + "manifest.pr");
    p2.add("allContrib", pagerankTemp + File.separator + "allContrib.pr");
    p2.add("lambda", Double.toString(lambda));
    stage.add(new Step(PRAllContribCollector.class , p2));

    //emits new PREntry's with the contribution to the linked pages
    Parameters p3 = new Parameters();
    p3.add("lambda", Double.toString(lambda));
    stage.add(new Step(PRMapper.class , p3));
    stage.add(new OutputStep(newPREntries));

    return stage;
  }

  public Stage getPRReduceStage() {
    Stage stage = new Stage("PageRankReduce");

    String newPREntries = "newPageRankEntries";
    String reducedPREntries = "reducedPageRankEntries";

    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
        newPREntries, new PREntry.DocNumOrder()));
    stage.add(new StageConnectionPoint(ConnectionPointType.Output,
        reducedPREntries, new PREntry.DocNumOrder()));

    stage.add(new InputStep(newPREntries));
    stage.add(Utility.getSorter(new PREntry.DocNumOrder()));
    stage.add(new Step(PRReducer.class));

    Parameters p = new Parameters();
    p.add("allContrib", pagerankTemp + File.separator + "allContrib.pr");
    stage.add(new Step(PRAllContribAdder.class, p));
    
    stage.add(new OutputStep(reducedPREntries));

    return stage;
  }

  public Stage getPRCleanupStage() {
    Stage stage = new Stage("PageRankCleanup");

    String reducedPREntries = "reducedPageRankEntries";
    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
        reducedPREntries, new PREntry.DocNumOrder()));

    stage.add(new InputStep(reducedPREntries));

    Parameters p = new Parameters();
    p.add("convergenceThreshold", Double.toString(convergenceThreshold));
    p.add("converged", pagerankTemp + File.separator + "converged.pr");
    p.add("entries", pagerankTemp + File.separator + "entries.pr");
    stage.add(new Step(PRDeltaCalculator.class, p));

    Parameters p2 = new Parameters();
    p2.add("entries", pagerankTemp + File.separator + "entries.pr");
    stage.add(new Step(PREntryWriter.class, p2));

    return stage;


  }
  //creates a pagerank iteration job
  public Job makeJob(Parameters p) throws IOException {

    Job job = new Job();

    this.pagerankTemp = new File(p.get("pagerankTemp")).getAbsolutePath();
    this.lambda = p.get("lambda", 0.1);
    this.convergenceThreshold = p.get("convergenceThreshold", 0.0001);

    job.add(getPRMapStage());
    job.add(getPRReduceStage());
    job.add(getPRCleanupStage()); 

    job.connect("PageRankMap", "PageRankReduce", ConnectionAssignmentType.Each);
    job.connect("PageRankReduce", "PageRankCleanup", ConnectionAssignmentType.Combined);
    return job;
  }

  
}
