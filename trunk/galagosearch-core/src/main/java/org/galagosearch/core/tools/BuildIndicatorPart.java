/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import org.galagosearch.core.index.DocumentIndicatorWriter;
import org.galagosearch.core.parse.IndicatorFileLineParser;
import org.galagosearch.core.types.DocumentIndicator;
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
public class BuildIndicatorPart {

  static PrintStream output;

  public Job getJob(Parameters p) throws IOException, ClassNotFoundException {
    File indexPath = new File(p.get("indexPath")).getAbsoluteFile(); // fail if no path.
    assert (indexPath.isDirectory());

    Parameters parserParams = new Parameters();
    List<Value> vs = p.list("inputPaths");
    for (Value v : vs) {
      parserParams.add("input", new File(v.toString()).getAbsolutePath());
    }

    Parameters writerParams = new Parameters();
    writerParams.add("filename", indexPath.getAbsolutePath() + File.separator + p.get("indicatorPart"));
    // ensure we set a default value - default default value is 'false'
    writerParams.add("default", p.get("default", "false"));

    Class indicatorExtractionStep = Class.forName(p.get("indicatorExtractor", "org.galagosearch.core.parse.IndicatorExtractor"));

    Stage stage = new Stage("Indexer");
    stage.add(new Step(IndicatorFileLineParser.class, parserParams));
    stage.add(new Step(indicatorExtractionStep, p));
    stage.add(Utility.getSorter(new DocumentIndicator.DocumentOrder()));
    stage.add(new Step(DocumentIndicatorWriter.class, writerParams));

    Job job = new Job();
    job.add(stage);

    return job;
  }

  public static void commandHelpBuildIndicator() {
    output.println("galago indicator [flags] <index> (<input>)+");
    output.println();

    output.println("  Builds a Galago StructuredIndex with TupleFlow, using one thread ");
    output.println("  for each CPU core on your computer.  While some debugging output ");
    output.println("  will be displayed on the screen, most of the status information will");
    output.println("  appear on a web page.  A URL should appear in the command output ");
    output.println("  that will direct you to the status page.");
    output.println();

    output.println("<input>:  Can be either a file or directory, and as many can be");
    output.println("          specified as you like.  Galago can read html, xml, txt, ");
    output.println("          arc (Heritrix), trectext, trecweb and corpus files.");
    output.println("          Files may be gzip compressed (.gz).");
    output.println("<index>:  The directory path of the index to produce.");
    output.println();

    output.println("Algorithm Flags:");
    output.println("  --links={true|false}:    Selects whether to collect anchor text ");
    output.println("                           [default=false]");
    output.println("  --printJob={plan|dot|none}: Simply prints the execution plan of a Tupleflow-based job then exits.");
    output.println("                              'dot' dumps a dot file that you can use to look at the execution graph.");
    output.println("                           [default=none]");
    output.println("  --stemming={true|false}: Selects whether to build stemmed inverted ");
    output.println("                           lists in addition to non-stemmed ones.");
    output.println("                           [default=true]");
    output.println("  --corpusPath=/path/for/corpus: Selects the location to output a corpus folder.");
    output.println("                           Note that this is optional, if no path is supplied,");
    output.println("                           then no corpus will be created.");
    output.println("                           [default=None]");
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
      commandHelpBuildIndicator();
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

    Job job;
    BuildIndicatorPart build = new BuildIndicatorPart();
    job = build.getJob(p);

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
