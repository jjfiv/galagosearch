// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map.Entry;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.galagosearch.core.index.DocumentLengthsReader;
import org.galagosearch.core.index.DocumentNameReader;
import org.galagosearch.core.index.GenericIndexReader;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.index.StructuredIndexPartReader;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.core.index.corpus.DocumentReader.DocumentIterator;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.index.corpus.DocumentReader;
import org.galagosearch.core.index.KeyIterator;
import org.galagosearch.core.index.StructuredIndexPartModifier;
import org.galagosearch.core.index.merge.MergeIndexes;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.FileOrderedReader;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorStore;
import org.galagosearch.tupleflow.execution.JobExecutor;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

/**
 * TODO: Make distributed jobs generate their own tmp directories, to avoid collisions.
 * @author trevor, sjh, irmarc
 */
public class App {

  protected PrintStream output;

  public App(PrintStream out) {
    output = out;
  }

  protected void commandHelpBatchSearch() {
    output.println("galago batch-search <args>");
    output.println();
    output.println("  Runs a batch of queries against an index and produces TREC-formatted");
    output.println("  output.  The output can be used with retrieval evaluation tools like");
    output.println("  galago eval (org.galagosearch.core.eval).");
    output.println();
    output.println("  Sample invocation:");
    output.println("     galago batch-search --index=/tmp/myindex --count=200 /tmp/queries");
    output.println();
    output.println("  Args:");
    output.println("     --index=path_to_your_index");
    output.println("     --count : Number of results to return for each query, default=1000");
    output.println();
    output.println("  Query file format:");
    output.println("    The query file is an XML file containing a set of queries.  Each query");
    output.println("    has text tag, which contains the text of the query, and a number tag, ");
    output.println("    which uniquely identifies the query in the output.");
    output.println();
    output.println("  Example query file:");
    output.println("  <parameters>");
    output.println("     <query>");
    output.println("        <number>CACM-408</number>");
    output.println("        <text>#combine(my query)</text>");
    output.println("     </query>");
    output.println("     <query>");
    output.println("        <number>WIKI-410</number>");
    output.println("        <text>#combine(another query)</text>");
    output.println("     </query>");
    output.println("  </parameters>");
  }

  protected void commandHelpBuild() {
    output.println("galago build[-fast] [flags] <index> (<input>)+");
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

  protected void commandHelpWindow() {
    output.println("galago window[-se] [flags] <index> (<input>)+");
    output.println();
    output.println("  Builds a Galago StructuredIndex window index file using TupleFlow. Program");
    output.println("  uses one thread for each CPU core on your computer.  While some debugging output ");
    output.println("  will be displayed on the screen, most of the status information will");
    output.println("  appear on a web page.  A URL should appear in the command output ");
    output.println("  that will direct you to the status page.");
    output.println();
    output.println("  window-se will produce an identical window index use a two-pass ");
    output.println("  space efficient algorithm. ");
    output.println();
    output.println("  Ordered or unordered windows can be generated. We match the #od and");
    output.println("  #uw operator definitions (See query language). Width of an ordered window");
    output.println("  is the maximum distance between words. Width of an unordered window is");
    output.println("  the differencebetween the location of the last word and the location of \n");
    output.println("  the first word.");
    output.println();
    output.println("<input>:  Can be either a file or directory, and as many can be");
    output.println("          specified as you like.  Galago can read html, xml, txt, ");
    output.println("          arc (Heritrix), trectext, trecweb and corpus files.");
    output.println("          Files may be gzip compressed (.gz).");
    output.println("<index>:  The directory path of the existing index (over the same corpus).");
    output.println();
    output.println("Algorithm Flags:");
    output.println("  --n={int >= 2}:          Selects the number of terms in each window (any reasonable value is possible).");
    output.println("                           [default = 2]");
    output.println("  --width={int >= 1}:      Selects the width of the window (Note: ordered windows are different to unordered windows).");
    output.println("                           [default = 1]");
    output.println("  --ordered={true|false}:  Selects ordered or unordered windows.");
    output.println("                           [default = true]");
    output.println("  --threshold={int >= 1}:  Selects the minimum number length of any inverted list.");
    output.println("                           Larger values will produce smaller indexes.");
    output.println("                           [default = 2]");
    output.println("  --usedocfreq={true|false}: Determines if the threshold is applied to term freq or doc freq.");
    output.println("                           [default = false]");
    output.println("  --stemming={true|false}: Selects whether to build a stemmed ngram inverted list.");
    output.println("                           [default=false]");
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

  protected void handleBuildTopdocs(String[] args) throws Exception {
    if (args.length < 5) {
      commandHelp(args[0]);
      return;
    }

    String[][] filtered = Utility.filterFlags(args);
    String[] flags = filtered[0];
    String[] nonFlags = Utility.subarray(filtered[1], 1);

    Parameters toJob = new Parameters();
    toJob.set("index", nonFlags[0]);
    toJob.set("part", nonFlags[1]);
    toJob.set("size", nonFlags[2]);
    toJob.set("minlength", nonFlags[3]);

    Job job;
    BuildTopDocs build = new BuildTopDocs();
    job = build.getIndexJob(toJob);

    Parameters toServer = new Parameters(flags);
    toServer.set("command", Utility.join(args, " "));
    String printJob = toServer.get("printJob", "none");
    if (printJob.equals("plan")) {
      System.out.println(job.toString());
      return;
    } else if (printJob.equals("dot")) {
      System.out.println(job.toDotString());
      return;
    }

    int hash = (int) toServer.get("distrib", 0);
    if (hash > 0) {
      job.properties.put("hashCount", Integer.toString(hash));
    }

    ErrorStore store = new ErrorStore();
    JobExecutor.runLocally(job, store, toServer);
    if (store.hasStatements()) {
      output.println(store.toString());
    }
  }

  protected void handleBuild(String[] args) throws Exception {
    if (args.length < 3) { // build index input
      commandHelpBuild();
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
    if (nonFlags[0].contains("fast")) {
      BuildFastIndex build = new BuildFastIndex();
      job = build.getIndexJob(p);

    } else {
      BuildIndex build = new BuildIndex();
      job = build.getIndexJob(p);
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

  protected void handleDoc(String[] args) throws IOException {
    if (args.length <= 2) {
      commandHelp(args[0]);
      return;
    }

    String indexPath = args[1];
    String identifier = args[2];
    DocumentReader reader = DocumentReader.getInstance(indexPath);

    Document document = reader.getDocument(identifier);
    output.println(document.text);
  }

  protected void handleDocId(String[] args) throws IOException {
    if (args.length <= 2) {
      commandHelp(args[0]);
      return;
    }

    String indexPath = args[1];
    String identifier = args[2];

    DocumentNameReader reader = new DocumentNameReader(indexPath);
    int docNum = reader.getDocumentId(identifier);
    output.println(docNum);
  }

  protected void handleDumpKeyValue(String[] args) throws IOException {
    if (args.length <= 2) {
      commandHelp(args[0]);
      return;
    }
    String key = args[2];
    output.printf("Dumping key: %s\n", key);
    StructuredIndexPartReader reader = StructuredIndex.openIndexPart(args[1]);
    KeyIterator iterator = reader.getIterator();
    if (iterator.moveToKey(Utility.fromString(key))) {
      ValueIterator vIter = iterator.getValueIterator();
      while (!vIter.isDone()) {
        output.printf("%s\n", vIter.getEntry());
        vIter.next();
      }
    }
  }

  protected void handleDumpIndex(String[] args) throws IOException {
    if (args.length <= 1) {
      commandHelp(args[0]);
      return;
    }

    StructuredIndexPartReader reader = StructuredIndex.openIndexPart(args[1]);
    KeyIterator iterator = reader.getIterator();
    while (!iterator.isDone()) {
      ValueIterator vIter = iterator.getValueIterator();
      while (!vIter.isDone()) {
        output.println(vIter.getEntry());
        vIter.next();
      }
      iterator.nextKey();
    }
  }

  protected void handleDumpModifier(String[] args) throws IOException {
    if (args.length <= 1) {
      commandHelp(args[0]);
      return;
    }

    StructuredIndexPartModifier modifier = StructuredIndex.openIndexModifier(args[1]);
    modifier.printContents(System.out);
  }

  protected void handleDumpCorpus(String[] args) throws IOException {
    if (args.length <= 1) {
      commandHelp(args[0]);
      return;
    }

    DocumentReader reader = DocumentReader.getInstance(args[1]);
    DocumentReader.DocumentIterator iterator = (DocumentIterator) reader.getIterator();

    while (!iterator.isDone()) {
      output.println("#IDENTIFIER: " + iterator.getKey());
      Document document = iterator.getDocument();
      output.println("#METADATA");
      for (Entry<String, String> entry : document.metadata.entrySet()) {
        output.println(entry.getKey() + "," + entry.getValue());
      }
      output.println("#TEXT");
      output.println(document.text);
      iterator.nextKey();
    }
  }

  protected void handleDumpConnection(String[] args) throws IOException {
    if (args.length <= 1) {
      commandHelp(args[0]);
      return;
    }

    FileOrderedReader reader = new FileOrderedReader(args[1]);
    Object o;
    while ((o = reader.read()) != null) {
      output.println(o);
    }
  }

  protected void handleDumpKeys(String[] args) throws IOException {
    if (args.length <= 1) {
      commandHelp(args[0]);
      return;
    }

    String keyType = "string";
    if (args.length > 2) {
      keyType = args[2];
    }
    String key = "";
    GenericIndexReader reader = GenericIndexReader.getIndexReader(args[1]);
    GenericIndexReader.Iterator iterator = reader.getIterator();
    while (!iterator.isDone()) {
      if (keyType.equals("string")) {
        key = Utility.toString(iterator.getKey());
      } else if (keyType.equals("int")) {
        key = Integer.toString(Utility.toInt(iterator.getKey()));
      } else if (keyType.equals("long")) {
        key = Long.toString(Utility.toLong(iterator.getKey()));
      } else if (keyType.equals("short")) {
        key = Short.toString(Utility.toShort(iterator.getKey()));
      } else {
        throw new IOException("Key type '" + keyType + "' unsupported.");
      }
      output.println(key);
      iterator.nextKey();
    }
  }

  protected void handleDumpLengths(String[] args) throws IOException {
    if (args.length <= 1) {
      commandHelp(args[0]);
      return;
    }

    DocumentLengthsReader reader = new DocumentLengthsReader(args[1]);
    DocumentLengthsReader.KeyIterator iterator = reader.getIterator();
    do {
      output.println(iterator.getValueString());
    } while (iterator.nextKey());
  }

  protected void handleDumpNames(String[] args) throws IOException {
    if (args.length <= 1) {
      commandHelp(args[0]);
      return;
    }

    DocumentNameReader reader = new DocumentNameReader(args[1]);
    DocumentNameReader.KeyIterator iterator = reader.getIterator();
    do {
      output.println(iterator.getValueString());
    } while (iterator.nextKey());
  }

  protected void handleMakeCorpus(String[] args) throws Exception {
    if (args.length <= 2) {
      commandHelp(args[0]);
      return;
    }

    String[][] filtered = Utility.filterFlags(args);

    String[] flags = filtered[0];
    String[] nonFlags = filtered[1];
    String outputCorpus = nonFlags[1];
    String[] docs = Utility.subarray(nonFlags, 2);

    Parameters p = new Parameters(flags);
    p.set("command", Utility.join(args, " "));
    p.add("corpusPath", outputCorpus);
    for (String doc : docs) {
      p.add("inputPaths", doc);
    }

    Job job;
    if (p.get("1", true)) {
      MakeCorpus mc = new MakeCorpus();
      job = mc.getMakeCorpusJob(p);
    } else {
      MakeCorpus mc = new MakeCorpus();
      job = mc.getMakeCorpusJob(p);
    }

    boolean printJob = Boolean.parseBoolean(p.get("printJob", "false"));
    if (printJob) {
      System.out.println(job.toString());
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

  /*
  protected void handleMergeIndexes(String[] args) throws Exception {
  // Remove 'merge-index' from the command.
  args = Utility.subarray(args, 1);
  
  if (args.length <= 0) {
  commandHelp("merge-index");
  return;
  }
  
  // handle --links and --stemming flags
  String[][] filtered = Utility.filterFlags(args);
  
  String[] flags = filtered[0];
  String[] nonFlags = filtered[1];
  String newIndex = nonFlags[0];
  String[] oldIndexes = Utility.subarray(nonFlags, 1);
  
  Parameters p = new Parameters(flags);
  p.set("command", Utility.join(args, " "));
  p.set("outputIndex", newIndex);
  for (String input : oldIndexes) {
  p.add("inputIndexes", input);
  }
  
  // ensure galagoTemp has been set
  p.set("galagoTemp", Utility.createGalagoTempDir(p.get("galagoTemp", "")).getAbsolutePath());
  
  MergeParallelIndexShards merger = new MergeParallelIndexShards();
  Job job = merger.getJob(p);
  
  boolean printJob = Boolean.parseBoolean(p.get("printJob", "false"));
  if (printJob) {
  System.out.println(job.toString());
  return;
  }
  
  int hash = (int) p.get("distrib", 0); // doesn't really matter in this case.
  if (hash > 0) // all other numbers don't make any sense
  {
  job.properties.put("hashCount", Integer.toString(hash));
  }
  
  ErrorStore store = new ErrorStore();
  JobExecutor.runLocally(job, store, p);
  if (store.hasStatements()) {
  output.println(store.toString());
  }
  }
   */
  protected void handleWindow(String[] args) throws Exception {
    if (args.length < 3) { // minimal usage: ngram index input
      commandHelpWindow();
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

    if (args[0].contains("se")) {
      p.add("spaceEfficient", "true");
    }

    Job job;
    BuildWindowIndex build = new BuildWindowIndex();
    job = build.getIndexJob(p);

    boolean printJob = Boolean.parseBoolean(p.get("printJob", "false"));
    if (printJob) {
      System.out.println(job.toString());
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

  protected void handleBatchSearch(String[] args) throws Exception {
    if (args.length <= 1) {
      commandHelpBatchSearch();
      return;
    }

    BatchSearch.run(Utility.subarray(args, 1), output);
  }

  protected class MappingHandler extends AbstractHandler {

    HashMap<String, Handler> handlers;

    public MappingHandler() {
      handlers = new HashMap<String, Handler>();
    }

    public void setHandler(String s, Handler h) {
      handlers.put(s, h);
    }

    public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch) throws IOException, ServletException {
      String path = request.getPathInfo();
      Handler h = handlers.get(path);
      if (h != null) {
        h.handle(target, request, response, dispatch);
      } else {
        throw new UnsupportedOperationException("Not supported yet.");
      }
    }
  }

  protected void handleSearch(Parameters p) throws Exception {
    Search search = new Search(p);
    int port = (int) p.get("port", 0);
    if (port == 0) {
      port = Utility.getFreePort();
    } else {
      if (!Utility.isFreePort(port)) {
        throw new IOException("Tried to bind to port " + port + " which is in use.");
      }
    }
    Server server = new Server(port);
    MappingHandler mh = new MappingHandler();
    mh.setHandler("/stream", new StreamContextHandler(search));
    mh.setHandler("/xml", new XMLContextHandler(search));
    mh.setHandler("/json", new JSONContextHandler(search));
    mh.setHandler("/web", new SearchWebHandler(search));
    server.addHandler(mh);
    server.start();
    output.println("Server: http://localhost:" + port);
  }

  protected void handleSearch(String[] args) throws Exception {
    if (args.length <= 1) {
      commandHelp("search");
      return;
    }

    // This is put in there to handle ONLY a parameter file, since
    // if you're loading multiple indexes you need to use a parameter file for it.
    if (args.length == 2 && args[1].endsWith(".xml")) {
      File f = new File(args[1]);
      Parameters p = new Parameters(f);
      handleSearch(p);

    } else {
      String[][] filtered = Utility.filterFlags(Utility.subarray(args, 1));
      String[] flags = filtered[0];
      String[] inputs = filtered[1];
      String indexPath = inputs[0];
      String[] corpora = Utility.subarray(inputs, 1);

      // Any flag marked '--parameters' marks a parameters file.
      // We trim that part of the flag off so that the Parameters object will
      // load it as a parameters file.
      for (int i = 0; i < flags.length; ++i) {
        flags[i] = flags[i].replace("--parameters=", "");
      }

      Parameters p = new Parameters(flags);
      p.add("index", indexPath);
      for (String corpus : corpora) {
        p.add("corpus", corpus);
      }
      handleSearch(p);
    }
  }

  public void handleXCount(String[] args) throws Exception {
    if (args.length <= 1) {
      commandHelp("xcount");
      return;
    }

    BatchSearch.xCount(args, output);
  }

  public void handleDocCount(String[] args) throws Exception {
    if (args.length <= 1) {
      commandHelp("doccount");
      return;
    }

    BatchSearch.docCount(args, output);
  }

  public void handleEval(String[] args) throws IOException {
    org.galagosearch.core.eval.Main.internalMain(Utility.subarray(args, 1), output);
  }

  public void usage() {
    output.println("Type 'galago help <command>' to get more help about any command,");
    output.println("   or 'galago help all' to see all the documentation at once.");
    output.println();

    output.println("Popular commands:");
    output.println("   build-fast");
    output.println("   search");
    output.println("   batch-search");
    output.println();

    output.println("All commands:");
    output.println("   batch-search");
    output.println("   build");
    output.println("   build-fast");
    output.println("   build-topdocs");
    output.println("   doc");
    output.println("   dump-connection");
    output.println("   dump-corpus");
    output.println("   dump-index");
    output.println("   dump-keys");
    output.println("   dump-keyvalue");
    output.println("   dump-lengths");
    output.println("   dump-modifier");
    output.println("   dump-names");
    output.println("   indicator");
    output.println("   eval");
    output.println("   make-corpus");
    output.println("   merge-index");
    output.println("   window");
    output.println("   window-se");
    //output.println("   pagerank");
    output.println("   search");
    output.println("   xcount");
    output.println("   doccount");
  }

  public void commandHelp(String command) throws IOException {
    if (command.equals("batch-search")) {
      commandHelpBatchSearch();
    } else if (command.equals("build") || command.equals("build-fast") || command.equals("build-parallel")) {
      commandHelpBuild();
    } else if (command.startsWith("window")) {
      commandHelpWindow();
    } else if (command.startsWith("merge-index")) {
      MergeIndexes.commandHelpMerge();
    } else if (command.startsWith("indicator")) {
      BuildIndicatorPart.commandHelpBuildIndicator();
    } else if (command.startsWith("pagerank")) {
      //PageRankApp.commandHelpPageRank();
    } else if (command.startsWith("build-topdocs")) {
      output.println("galago build-topdocs <index> <part> <size> <minlength>");
      output.println();
      output.println("Constructs topdoc lists consisting of <size> documents,");
      output.println("and only for lists longer than <minlength>. Note that");
      output.println("<index> needs to point an index, while <part> is the part to scan.");
    } else if (command.equals("doc")) {
      output.println("galago doc <corpus> <identifier>");
      output.println();
      output.println("  Prints the full text of the document named by <identifier>.");
      output.println("  The document is retrieved from a Corpus file named <corpus>.");
    } else if (command.equals("doc-id")) {
      output.println("galago doc <documentNames.ReverseLookup> <identifier>");
      output.println();
      output.println("  Prints the internal document id of the document named by <identifier>.");
    } else if (command.equals("dump-connection")) {
      output.println("galago dump-connection <connection-file>");
      output.println();
      output.println("  Dumps tuples from a Galago TupleFlow connection file in ");
      output.println("  CSV format.  This can be useful for debugging strange problems ");
      output.println("  in a TupleFlow execution.");
    } else if (command.equals("dump-corpus")) {
      output.println("galago dump-corpus <corpus>");
      output.println();
      output.println("  Dumps all documents from a corpus file to stdout.");
    } else if (command.equals("dump-modifier")) {
      output.println("galago dump-modifier <modifier file>");
      output.println();
      output.println("  Dumps the contents of the specified modifier file.");
    } else if (command.equals("dump-index")) {
      output.println("galago dump-index <index-part>");
      output.println();
      output.println("  Dumps inverted list data from any index file in a StructuredIndex");
      output.println("  (That is, any index that has a readerClass that's a subclass of ");
      output.println("  StructuredIndexPartReader).  Output is in CSV format.");
    } else if (command.equals("dump-keys")) {
      output.println("galago dump-keys <indexwriter-file>");
      output.println();
      output.println("  Dumps all keys from any file created by IndexWriter.  This includes");
      output.println("  corpus files and all index files built by Galago.");
    } else if (command.equals("dump-keyvalue")) {
      output.println("galago dump-keys <indexwriter-file> <key>");
      output.println();
      output.println("  Dumps all data associated with a particular key from a file created by IndexWriter.  This includes");
      output.println("  corpus files and all index files built by Galago.");
    } else if (command.equals("dump-lengths")) {
      output.println("galago dump-lengths <document-lengths-file>");
      output.println();
      output.println("  Dumps all lengths from a length file created by DocumentLengthsWriter.");
      output.println("  This is only for the documentLengths file produced by a Galago indexing job.");
    } else if (command.equals("dump-names")) {
      output.println("galago dump-names <document-names-file>");
      output.println();
      output.println("  Dumps all names from a names folder created by DocumentNamesWriter2.");
      output.println("  This is only for the documentNames folder produced by a Galago indexing job.");
    } else if (command.equals("xcount")) {
      output.println("galago xcount --x=<countable-query> --index=<index> ");
      output.println();
      output.println("  Returns the number of times the countable-query occurs.");
      output.println("  More than one index and expression can be specified.");
      output.println("  Examples of countable-expressions: terms, ordered windows and unordered windows.");
    } else if (command.equals("doccount")) {
      output.println("galago doccount --x=<countable-query> --index=<index> ");
      output.println();
      output.println("  Returns the number of documents that contain the countable-query.");
      output.println("  More than one index and expression can be specified.");
      output.println("  Examples of countable-expressions: terms, ordered windows and unordered windows.");
    } else if (command.equals("eval")) {
      org.galagosearch.core.eval.Main.usage(output);
    } else if (command.equals("make-corpus")) {
      output.println("galago make-corpus [flags]+ <corpus> (<input>)+");
      output.println();
      output.println("  Copies documents from input files into a corpus file.  A corpus");
      output.println("  structure is required to use any of the document lookup features in ");
      output.println("  Galago, like printing snippets of search results.");
      output.println();
      output.println("<corpus>: Newly constructed corpus output directory");
      output.println();
      output.println("<input>:  Can be either a file or directory, and as many can be");
      output.println("          specified as you like.  Galago can read html, xml, txt, ");
      output.println("          arc (Heritrix), trectext, trecweb and corpus files.");
      output.println("          Files may be gzip compressed (.gz).");
      output.println();
      output.println("  --corpusFormat={folder|file}: Selects which format of corpus to produce.");
      output.println("                           File is a single file corpus. Folder is a folder of data files with an index.");
      output.println("                           The folder structure can be produce in a parallel manner.");
      output.println("                           [default=folder]");
      output.println("  --printJob={true|false}: Simply prints the execution plan of a Tupleflow-based job then exits.");
      output.println("                           [default=none]");
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
      output.println("                           [default=0]");
      output.println("  --distrib={int > 1}:     Selects the number of simultaneous jobs to create");
      output.println("                           [default = 10]");
    } else if (command.equals("search")) {
      output.println("galago search [--parameters=<filename>] <index> <corpus>+");
      output.println();
      output.println("  Starts a web interface for searching an index interactively.");
      output.println("  The URL to use in your web browser will appear in the command ");
      output.println("  output.  Cancel the process (Control-C) to quit.");
      output.println();
      output.println("  If you specify a parameters file, you can direct Galago to load ");
      output.println("  extra operators or traversals from your own jar files.  See ");
      output.println("  the documentation for ");
      output.println("  org.galagosearch.core.retrieval.structured.FeatureFactory for more");
      output.println("  information.");
      output.println();
      output.println("  Parameters availiable:");
      output.println("   --corpus={file path} : corpus file path");
      output.println("   --index={file path}  : index file path");
      output.println("   --index={url}        : galago search url (for distributed retrieval)");
      output.println("   --port={int<65000}   : port number for web retrieval. ");
    } else if (command.equals("all")) {
      String[] commands = {"batch-search", "build", "doc", "dump-connection", "dump-corpus",
        "dump-index", "dump-keys", "eval", "make-corpus", "search"};
      for (String c : commands) {
        commandHelp(c);
        output.println();
      }
    } else {
      usage();
    }
  }

  public void run(String[] args) throws Exception {
    if (args.length < 1) {
      usage();
      return;
    }

    String command = args[0];

    if (command.equals("help") && args.length > 1) {
      commandHelp(args[1]);
    } else if (command.equals("batch-search")) {
      handleBatchSearch(args);
    } else if (command.equals("build")) {
      handleBuild(args);
    } else if (command.equals("build-fast")) {
      handleBuild(args);
    } else if (command.equals("build-topdocs")) {
      handleBuildTopdocs(args);
    } else if (command.equals("doc")) {
      handleDoc(args);
    } else if (command.equals("doc-id")) {
      handleDocId(args);
    } else if (command.equals("dump-connection")) {
      handleDumpConnection(args);
    } else if (command.equals("dump-corpus")) {
      handleDumpCorpus(args);
    } else if (command.equals("dump-modifier")) {
      handleDumpModifier(args);
    } else if (command.equals("dump-index")) {
      handleDumpIndex(args);
    } else if (command.equals("dump-keys")) {
      handleDumpKeys(args);
    } else if (command.equals("dump-keyvalue")) {
      handleDumpKeyValue(args);
    } else if (command.equals("dump-lengths")) {
      handleDumpLengths(args);
    } else if (command.equals("dump-names")) {
      handleDumpNames(args);
    } else if (command.equals("indicator")) {
      BuildIndicatorPart.main(args);
    } else if (command.equals("make-corpus")) {
      handleMakeCorpus(args);
    } else if (command.equals("merge-index")) {
      MergeIndexes.main(args);
    } else if (command.equals("window")) {
      handleWindow(args);
    } else if (command.equals("window-se")) {
      handleWindow(args);
    } else if (command.equals("pagerank")) {
      throw new UnsupportedOperationException("Need to re-implement");
      //PageRankApp.main(args);
    } else if (command.equals("search")) {
      handleSearch(args);
    } else if (command.equals("xcount")) {
      handleXCount(args);
    } else if (command.equals("doccount")) {
      handleDocCount(args);
    } else if (command.equals("eval")) {
      handleEval(args);
    } else {
      usage();
    }
  }

  public static void main(String[] args) throws Exception {
    new App(System.out).run(args);
  }
}
