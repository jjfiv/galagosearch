// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.pagerank.program;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorStore;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.execution.JobExecutor;

/**
 * Main application for the pagerank module
 *
 * @author sjh
 */
public class PageRankApp {

  private static PrintStream output;

  public static void commandHelpPageRank() {
    output.println("galago pagerank [flags] <index> (<input>)+");
    output.println();
    output.println("  Builds a Galago Structured Index pagerank file with TupleFlow, using");
    output.println("  one thread for each CPU core on your computer.  While some debugging output ");
    output.println("  will be displayed on the screen, most of the status information will");
    output.println("  appear on a web page.  A URL should appear in the command output ");
    output.println("  that will direct you to the status page.");
    output.println();

    output.println("<input>:  Can be either a file or directory, and as many can be");
    output.println("          specified as you like.  Galago can read html, xml, txt, ");
    output.println("          arc (Heritrix), trectext, trecweb and corpus files.");
    output.println("          Files may be gzip compressed (.gz).");
    output.println("<index>:  The directory path of the existing index (over the same corpus).");
    output.println();
    output.println("Flags:");
    output.println("  --printJob={true|false}: Simply prints the execution plan of a Tupleflow-based job then exits.");
    output.println("                           [default=false]");
    output.println("  --mode={local|threaded|drmaa}: Selects which executor to use ");
    output.println("                           [default=local]");
    output.println("  --galagoTemp=/path/to/temp/dir/: Sets the galago temp dir ");
    output.println("                           [default = uses folders specified in ~/.galagotmp or java.io.tmpdir]");
    output.println("  --pagerankTemp=/path/to/temp/dir/: Sets the pagerank temp dir ");
    output.println("                           [default = uses folders specified in ~/.galagotmp or java.io.tmpdir]");
    output.println("  --deleteOutput={0|1|2}:  Selects how much of the galago temp dir to delete");
    output.println("                           0 --> keep all data");
    output.println("                           1 --> delete all data + keep jobs directory (only useful for drmaa mode)");
    output.println("                           2 --> delete entire temp directory");
    output.println("                           [default=2]");
    output.println("  --distrib={int > 1}:     Selects the number of simultaneous jobs to create");
    output.println("                           [default = 10]");
    output.println("  --appMode={init|iter|both}: Selects which part of the algorithm to run");
    output.println("                           [default = both]");
    output.println("  --lambda={double [0->1]}: Selects the lambda for this algorithm");
    output.println("                           [default = 0.1]");
    output.println("  --convergance={double [0->1]}: Selects the convergence property for the algorithm");
    output.println("                           [default = 0.0001]");
    output.println("  --maxIterations={int >0}: Selects the maximum number of iterations");
    output.println("                           [default = Integer.MAX_VALUE]");

  }

  public static void main(String[] args) throws Exception {
    output = System.out;

    if (args.length < 3) { // pagerank index input
      commandHelpPageRank();
      return;
    }

    // handle --links and --stemming flags
    String[][] filtered = Utility.filterFlags(args);

    String[] flags = filtered[0];
    String[] nonFlags = filtered[1];
    String indexName = nonFlags[1];
    String[] docs = Utility.subarray(nonFlags, 2);

    Parameters p = new Parameters(flags);
    p.set("indexPath", indexName);
    for (String doc : docs) {
      p.add("inputPaths", doc);
    }
    String pagerankFolder = p.get("pagerankTemp", "");
    File pgrankTemp = Utility.createGalagoTempDir(pagerankFolder);
    p.set("pagerankTemp", pgrankTemp.getAbsolutePath());
    String tempFolderPath = p.get("galagoTemp", "");
    File tempFolder = Utility.createGalagoTempDir(tempFolderPath);
    p.set("galagoTemp", tempFolder.getAbsolutePath());


    ErrorStore store = runPageRankJob(p);

    if (store.hasStatements()) {
      output.println(store.toString());
    }


  }

  public static ErrorStore runPageRankJob(Parameters p) throws Exception {

    ErrorStore store = new ErrorStore();

    boolean printJob = Boolean.parseBoolean(p.get("printJob","false"));
    int deleteOutput = (int) p.get("deleteOutput", 2);
    int hash = (int) p.get("distrib", 0);
    int maxIterations = (int) p.get("maxIterations", Integer.MAX_VALUE);
    File pagerankFolder = new File(p.get("pagerankTemp"));
    File tempFolder = new File(p.get("galagoTemp"));

    String appMode = p.get("appMode", "both");
    boolean init = false;
    boolean iter = false;
    if (appMode.contains("init") || appMode.contains("both")) {
      init = true;
    }
    if (appMode.contains("iter") || appMode.contains("both")) {
      iter = true;
    }

    System.err.println("init =" + init);
    System.err.println("iter =" + iter);

    Job initJob = null;
    Job iterJob = null;
    Job closeJob = null;

    if (init) {
      initJob = new PageRankInit().makeJob(p);
      if (hash > 0) {
        initJob.properties.put("hashCount", Integer.toString(hash));
      }
      if (printJob) {
        System.out.println("--- initJob ---");
        System.out.println(initJob.toString());
      }
    }

    if (iter) {
      iterJob = new PageRankIteration().makeJob(p);
      closeJob = new PageRankCleanup().makeJob(p);
      if (hash > 0) {
        iterJob.properties.put("hashCount", Integer.toString(hash));
        closeJob.properties.put("hashCount", Integer.toString(hash));
      }
      if (printJob) {
        System.out.println("--- iterJob ---");
        System.out.println(iterJob.toString());
        System.out.println("--- closeJob ---");
        System.out.println(iterJob.toString());

        return store;
      }
    }

    if (init) {
      System.err.println("Init Phase:");
      p.set("galagoTemp", makeGalagoTemp(tempFolder, "init").getCanonicalPath());
      JobExecutor.runLocally(initJob, store, p);
      if (store.hasStatements()) {
        return store;
      }
    }

    int iteration = 0;
    if (iter) {
      System.err.println("Iter Phase:");
      while (!converged(pagerankFolder)) {
        if(iteration >= maxIterations){
          break;
        }

        p.set("galagoTemp", makeGalagoTemp(tempFolder, "iteration-"+iteration).getCanonicalPath());
        JobExecutor.runLocally(iterJob, store, p);
        if (store.hasStatements()) {
          return store;
        }

        iteration++;
      }
      
      System.err.println("Cleanup Phase:");
      p.set("galagoTemp", makeGalagoTemp(tempFolder, "closer").getCanonicalPath());
      JobExecutor.runLocally(closeJob, store, p);

      if (store.hasStatements()) {
        return store;
      }
    }

    if(deleteOutput == 2){
      Utility.deleteDirectory(tempFolder);
    }
    
    return store;
  }

  private static boolean converged(File tempDir) {
    File f = new File(tempDir.getAbsolutePath() + File.separator + "converged.pr");
    return f.exists();
  }

  private static File makeGalagoTemp(File tempDir, String folderName) {
    String newTempGal = tempDir.getAbsoluteFile() + File.separator + folderName;
    File newTempGal2 = new File(newTempGal);
    newTempGal2.mkdirs();
    return newTempGal2;
  }
}
