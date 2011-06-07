/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import org.galagosearch.core.index.DocumentIndicatorWriter;
import org.galagosearch.core.index.DocumentPriorWriter;
import org.galagosearch.core.parse.FileLineParser;
import org.galagosearch.core.types.DocumentIndicator;
import org.galagosearch.core.types.NumberWordProbability;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorStore;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.execution.JobExecutor;
import org.galagosearch.tupleflow.execution.Stage;
import org.galagosearch.tupleflow.execution.Step;

/**
 * Reads a file in a standard format
 * Writes an indicator index from the data in the file
 *  
 * Format : Document Identifier \t [true | false] \n
 * 
 * Document Identifiers not in index 
 *  will not be ignored.
 * 
 * @author sjh
 */
public class BuildSpecialPart {

  static PrintStream output;

  public Job getIndicatorJob(Parameters p) throws IOException, ClassNotFoundException {
    String indexPath = new File(p.get("indexPath")).getAbsolutePath(); // fail if no path.
    p.set("indexPath", indexPath);
    assert (new File(indexPath).isDirectory());

    Parameters parserParams = new Parameters();
    List<Value> vs = p.list("inputPaths");
    for (Value v : vs) {
      parserParams.add("input", new File(v.toString()).getAbsolutePath());
    }

    Parameters writerParams = new Parameters();
    writerParams.add("filename", indexPath + File.separator + p.get("partName"));
    // ensure we set a default value - default default value is 'false'
    writerParams.add("default", p.get("default", "false"));

    Class indicatorExtractionStep = Class.forName(p.get("extractor", "org.galagosearch.core.parse.IndicatorExtractor"));

    Stage stage = new Stage("Indexer");
    stage.add(new Step(FileLineParser.class, parserParams));
    stage.add(new Step(indicatorExtractionStep, p));
    stage.add(Utility.getSorter(new DocumentIndicator.DocumentOrder()));
    stage.add(new Step(DocumentIndicatorWriter.class, writerParams));

    Job job = new Job();
    job.add(stage);

    return job;
  }


  public Job getPriorJob(Parameters p) throws ClassNotFoundException {
    String indexPath = new File(p.get("indexPath")).getAbsolutePath(); // fail if no path.
    p.set("indexPath", indexPath);
    assert (new File(indexPath).isDirectory());

    Parameters parserParams = new Parameters();
    List<Value> vs = p.list("inputPaths");
    for (Value v : vs) {
      parserParams.add("input", new File(v.toString()).getAbsolutePath());
    }

    Parameters writerParams = new Parameters();
    writerParams.add("filename", indexPath + File.separator + p.get("partName"));
    // ensure we set a default value - default default value is 'false'
    writerParams.add("default", p.get("default", Double.toString(Double.NEGATIVE_INFINITY)));

    Class priorExtractionStep = Class.forName(p.get("extractor", "org.galagosearch.core.parse.PriorExtractor"));

    Stage stage = new Stage("Indexer");
    stage.add(new Step(FileLineParser.class, parserParams));
    stage.add(new Step(priorExtractionStep, p));
    stage.add(Utility.getSorter(new NumberWordProbability.NumberOrder()));
    stage.add(new Step(DocumentPriorWriter.class, writerParams));

    Job job = new Job();
    job.add(stage);

    return job;
  }
  
  
  public static void commandHelpBuildSpecial() {
    output.println("galago build-special [flags] <index> (<input>)+");
    output.println();

    output.println("  Builds a Galago Structured Index Part file with TupleFlow, ");
    output.println("  Can build either an indicator part or prior part.");
    output.println();
    output.println("<indicator-input>:  One or more indicator files in format:");
    output.println("           < document-identifier \t [true | false] >");
    output.println();
    output.println("<prior-input>:  One or more indicator files in format:");
    output.println("           < document-identifier \t [log-probability] >");
    output.println();
    output.println("<index>:  The directory path of the index to add to.");
    output.println();
    output.println("Algorithm Flags:");
    output.println("  --type={indicator|prior}: Sets the type of index part to build.");
    output.println("                            [default=prior]");
    output.println();
    output.println("  --partName={String}:      Sets the name of index part.");
    output.println("                 indicator: [default=prior]");
    output.println("                     prior: [default=indicator]");
    output.println();
    output.println("  --extractor={java class}: Sets the class that extracts boolean values for each input line.");
    output.println("                 indicator: [default=org.galagosearch.core.parse.IndicatorExtractor]");
    output.println("                     prior: [default=org.galagosearch.core.parse.PriorExtractor]");
    output.println();
    output.println("  --default={true|false|float}: Sets the default value for the index part.");
    output.println("                 indicator: [default=false]");
    output.println("                     prior: [default=-inf");
    output.println();
    output.println("  --priorType={raw|prob|logprob}: Sets the type of prior to read. (Only for prior parts)");
    output.println("                            [default=raw]");
    output.println();
    output.println();
    output.println("Tupleflow Flags:");
    output.println("  --printJob={true|false}: Simply prints the execution plan of a Tupleflow-based job then exits.");
    output.println("                           [default=false]");
    output.println("  --mode={local|threaded|drmaa}: Selects which executor to use ");
    output.println("                           [default=local]");
    output.println("  --port={int<65000} :     port number for web based progress monitoring. ");
    output.println("                           [default=randomly selected free port]");
    output.println("  --galagoTemp=/path/to/temp/dir/: Sets the galago temp dir ");
    output.println("                           [default = uses folders specified in ~/.galagotmp or java.io.tmpdir]");
    output.println("  --deleteOutput={0|1|2}:    Selects how much of the galago temp dir to delete");
    output.println("                           0 --> keep all data");
    output.println("                           1 --> delete all data + keep jobs directory (only useful for drmaa mode)");
    output.println("                           2 --> delete entire temp directory");
    output.println("                           [default=2]");
    output.println("  --distrib={int > 1}:     Selects the number of simultaneous jobs to create");
    output.println("                           [default = 10]");
  }

  public static void main(String[] args) throws Exception {
    output = System.out;
    if (args.length < 3) { // build index input
      commandHelpBuildSpecial();
      return;
    }

    String[][] filtered = Utility.filterFlags(args);

    String[] flags = filtered[0];
    String[] nonFlags = filtered[1];
    String indexName = nonFlags[1];
    String[] docs = Utility.subarray(nonFlags, 2);

    Parameters p = new Parameters(flags);
    p.set("command", Utility.join(args, " "));
    p.add("indexPath", indexName);
    for (String doc : docs) {
      p.add("inputPaths", doc);
    }

    Job job = null;
    BuildSpecialPart build = new BuildSpecialPart();
    String type = p.get("type", "prior");
    if(type.equals("indicator")){
      job = build.getIndicatorJob(p);
    } else if(type.equals("prior")){
      job = build.getPriorJob(p);
    }

    String printJob = p.get("printJob", "none");
    if (printJob.equals("plan")) {
      System.out.println(job.toString());
      return;
    } else if (printJob.equals("dot")) {
      System.out.println(job.toDotString());
      return;
    }

    int hash = (int) p.get("distrib", 0);
    if (hash > 0) {
      job.properties.put("hashCount", Integer.toString(hash));
    }

    ErrorStore store = new ErrorStore();
    JobExecutor.runLocally(job, store, p);
    if (store.hasStatements()) {
      output.println(store.toString());
    }
  }
}
