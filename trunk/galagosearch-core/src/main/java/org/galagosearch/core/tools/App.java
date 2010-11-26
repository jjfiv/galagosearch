// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Map.Entry;
import org.galagosearch.core.index.DocumentLengthsReader;
import org.galagosearch.core.index.DocumentNameReader;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.index.StructuredIndexPartReader;
import org.galagosearch.core.pagerank.program.PageRankApp;
import org.galagosearch.core.parse.CorpusReader;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.parse.DocumentIndexReader;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.core.mergeindex.parallel.MergeParallelIndexShards;
import org.galagosearch.core.parse.DocumentReader;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.structured.IndexIterator;
import org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator;
import org.galagosearch.core.store.DocumentIndexStore;
import org.galagosearch.core.store.DocumentStore;
import org.galagosearch.core.store.NullStore;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.FileOrderedReader;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorStore;
import org.galagosearch.tupleflow.execution.JobExecutor;
import org.mortbay.jetty.Server;

/**
 *
 * @author trevor, sjh, irmarc
 */
public class App {

  private PrintStream output;

  public App(PrintStream out) {
    output = out;
  }

  private void commandHelpBatchSearch() {
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

  private void commandHelpBuild() {
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
    output.println("Flags:");
    output.println("  --links={true|false}:    Selects whether to collect anchor text ");
    output.println("                           [default=false]");
    output.println("  --printJob={true|false}: Simply prints the execution plan of a Tupleflow-based job then exits.");
    output.println("                           [default=false]");
    output.println("  --stemming={true|false}: Selects whether to build stemmed inverted ");
    output.println("                           lists in addition to non-stemmed ones.");
    output.println("                           [default=true]");
    output.println("  --corpusPath=/path/for/corpus: Selects the location to output a corpus folder.");
    output.println("                           Note that this is optional, if no path is supplied,");
    output.println("                           then no corpus will be created.");
    output.println("                           [default=None]");
    output.println("  --mode={local|threaded|drmaa}: Selects which executor to use ");
    output.println("                           [default=local]");
    output.println("  --galagoTemp=/path/to/temp/dir/: Sets the galago temp dir ");
    output.println("                           [default = uses folders specified in ~/.galagotmp or java.io.tmpdir]");
    output.println("  --deleteOutput={0|1|2}:    Selects how much of the galago temp dir to delete");
    output.println("                           0 --> keep all data");
    output.println("                           1 --> delete all data + keep jobs directory (only useful for drmaa mode)");
    output.println("                           2 --> delete entire temp directory");
    output.println("                           [default=0]");
    output.println("  --distrib={int > 1}:     Selects the number of simultaneous jobs to create");
    output.println("                           [default = 10]");
  }

  private void handleBuild(String[] args) throws Exception {
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
    p.add("indexPath", indexName);
    for (String doc : docs) {
      p.add("inputPaths", doc);
    }

    boolean printJob = Boolean.parseBoolean(p.get("printJob", "false"));
    int deleteOutput = Integer.parseInt(p.get("deleteOutput", "2"));
    int hash = (int) p.get("distrib", 0);
    String mode = p.get("mode", "local");
    String tempFolderPath = p.get("galagoTemp", "");
    File tempFolder = Utility.createGalagoTempDir(tempFolderPath);

    Job job;
    if (nonFlags[0].contains("fast")) {
      BuildFastIndex build = new BuildFastIndex();
      job = build.getIndexJob(p);
    } else {
      BuildIndex build = new BuildIndex();
      job = build.getIndexJob(p);
    }

    if (printJob) {
      System.out.println(job.toString());
      return;
    }

    ErrorStore store = new ErrorStore();

    if (hash > 0) {
      job.properties.put("hashCount", Integer.toString(hash));
    }

    JobExecutor.runLocally(job, store, deleteOutput, mode, tempFolder);
    if (store.hasStatements()) {
      output.println(store.toString());
    }
  }

  private void handleDoc(String[] args) throws IOException {
    if (args.length <= 2) {
      commandHelp(args[0]);
      return;
    }

    String indexPath = args[1];
    String identifier = args[2];
    DocumentReader reader;
    if (CorpusReader.isCorpus(indexPath)) {
      reader = new CorpusReader(indexPath);
    } else {
      reader = new DocumentIndexReader(indexPath);
    }

    Document document = reader.getDocument(identifier);
    output.println(document.text);
  }

  private void handleDumpIndex(String[] args) throws IOException {
    if (args.length <= 1) {
      commandHelp(args[0]);
      return;
    }

    StructuredIndexPartReader reader = StructuredIndex.openIndexPart(args[1]);
    IndexIterator iterator = reader.getIterator();
    do {
      output.println(iterator.getRecordString());
    } while (iterator.nextRecord());
  }

  private void handleDumpCorpus(String[] args) throws IOException {
    if (args.length <= 1) {
      commandHelp(args[0]);
      return;
    }

    DocumentReader reader;
    if (CorpusReader.isCorpus(args[1])) {
      reader = new CorpusReader(args[1]);
    } else {
      reader = new DocumentIndexReader(args[1]);
    }
    DocumentReader.DocumentIterator iterator = reader.getIterator();

    while (!iterator.isDone()) {
      output.println("#IDENTIFIER: " + iterator.getKey());
      Document document = iterator.getDocument();
      output.println("#METADATA");
      for (Entry<String, String> entry : document.metadata.entrySet()) {
        output.println(entry.getKey() + "," + entry.getValue());
      }
      output.println("#TEXT");
      output.println(document.text);
      iterator.nextDocument();
    }
  }

  private void handleDumpConnection(String[] args) throws IOException {
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

  private void handleDumpKeys(String[] args) throws IOException {
    if (args.length <= 1) {
      commandHelp(args[0]);
      return;
    }

    IndexReader reader = new IndexReader(args[1]);
    IndexReader.Iterator iterator = reader.getIterator();
    while (!iterator.isDone()) {
      output.println(iterator.getKey());
      iterator.getValueString();
      iterator.nextKey();
    }
  }
  private void handleDumpLengths(String[] args) throws IOException {
    if (args.length <= 1) {
      commandHelp(args[0]);
      return;
    }

    DocumentLengthsReader reader = new DocumentLengthsReader(args[1]);
    NumberedDocumentDataIterator iterator = reader.getIterator();
    do {
      output.println(iterator.getRecordString());
    } while (iterator.nextRecord());
  }

  private void handleDumpNames(String[] args) throws IOException {
    if (args.length <= 1) {
      commandHelp(args[0]);
      return;
    }

    DocumentNameReader reader = new DocumentNameReader(args[1]);
    NumberedDocumentDataIterator iterator = reader.getNumberOrderIterator();
    do {
      output.println(iterator.getRecordString());
    } while (iterator.nextRecord());
  }

  private void handleMakeCorpus(String[] args) throws Exception {
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
    p.add("corpusPath", outputCorpus);
    for (String doc : docs) {
      p.add("inputPaths", doc);
    }

    boolean printJob = Boolean.parseBoolean(p.get("printJob", "false"));
    int deleteOutput = Integer.parseInt(p.get("deleteOutput", "2"));
    int hash = (int) p.get("distrib", 0);
    String mode = p.get("mode", "local");
    String tempFolderPath = p.get("galagoTemp", "");
    File tempFolder = Utility.createGalagoTempDir(tempFolderPath);

    MakeCorpus mc = new MakeCorpus();
    Job job = mc.getMakeCorpusJob(p);

    if (printJob) {
      System.out.println(job.toString());
      return;
    }

    if (hash > 0) {
      job.properties.put("hashCount", Integer.toString(hash));
    }

    ErrorStore store = new ErrorStore();
    JobExecutor.runLocally(job, store, deleteOutput, mode, tempFolder);
    if (store.hasStatements()) {
      output.println(store.toString());
    }
  }

  private void handleMergeIndexes(String[] args) throws Exception {
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
    p.set("outputIndex", newIndex);
    for (String input : oldIndexes) {
      p.add("inputIndexes", input);
    }

    boolean printJob = Boolean.parseBoolean(p.get("printJob", "false"));
    int deleteOutput = (int) p.get("deleteOutput", 2);
    int hash = (int) p.get("distrib", 0); // doesn't really matter in this case.
    String mode = p.get("mode", "local");
    String tempFolderPath = p.get("galagoTemp", "");
    File tempFolder = Utility.createGalagoTempDir(tempFolderPath);
    p.set("galagoTemp", tempFolder.getAbsolutePath());

    MergeParallelIndexShards merger = new MergeParallelIndexShards();
    Job job = merger.getJob(p);

    if (printJob) {
      System.out.println(job.toString());
      return;
    }

    ErrorStore store = new ErrorStore();

    if (hash > 0) // all other numbers don't make any sense
    {
      job.properties.put("hashCount", Integer.toString(hash));
    }

    JobExecutor.runLocally(job, store, deleteOutput, mode, tempFolder);
    if (store.hasStatements()) {
      output.println(store.toString());
    }
  }

  private void handleBatchSearch(String[] args) throws Exception {
    if (args.length <= 1) {
      commandHelpBatchSearch();
      return;
    }

    BatchSearch.run(Utility.subarray(args, 1), output);
  }

  private void handleSearch(Retrieval retrieval, DocumentStore store) throws Exception {
    Search search = new Search(retrieval, store);
    int port = Utility.getFreePort();
    Server server = new Server(port);
    server.addHandler(new SearchWebHandler(search));
    server.start();
    output.println("Server: http://localhost:" + port);
  }

  private DocumentStore getDocumentStore(String[] corpusFiles) throws IOException {
    DocumentStore store = null;
    if (corpusFiles.length > 0) {
      ArrayList<DocumentReader> readers = new ArrayList<DocumentReader>();
      for (int i = 0; i < corpusFiles.length; ++i) {
        if (CorpusReader.isCorpus(corpusFiles[i])) {
          readers.add(new CorpusReader(corpusFiles[i]));
        } else {
          readers.add(new DocumentIndexReader(corpusFiles[i]));
        }
      }
      store = new DocumentIndexStore(readers);
    } else {
      store = new NullStore();
    }
    return store;
  }

  private void handleSearch(String[] args) throws Exception {
    if (args.length <= 1) {
      commandHelp("search");
      return;
    }

    String indexPath = args[1];
    String[][] filtered = Utility.filterFlags(Utility.subarray(args, 2));
    String[] flags = filtered[0];
    String[] corpusFiles = filtered[1];

    // Any flag marked '--parameters' marks a parameters file.
    // We trim that part of the flag off so that the Parameters object will
    // load it as a parameters file.
    for (int i = 0; i < flags.length; ++i) {
      flags[i] = flags[i].replace("--parameters=", "");
    }

    Parameters p = new Parameters(flags);
    Retrieval retrieval = Retrieval.instance(indexPath, p);
    handleSearch(retrieval, getDocumentStore(corpusFiles));
  }

  public void handleEval(String[] args) throws IOException {
    org.galagosearch.core.eval.Main.internalMain(Utility.subarray(args, 1), output);
  }

  public void usage() {
    output.println("Type 'galago help <command>' to get more help about any command,");
    output.println("   or 'galago help all' to see all the documentation at once.");
    output.println();

    output.println("Popular commands:");
    output.println("   build");
    output.println("   search");
    output.println("   batch-search");
    output.println();

    output.println("All commands:");
    output.println("   batch-search");
    output.println("   build");
    output.println("   build-fast");
    output.println("   doc");
    output.println("   dump-connection");
    output.println("   dump-corpus");
    output.println("   dump-index");
    output.println("   dump-keys");
    output.println("   dump-lengths");
    output.println("   dump-names");
    output.println("   eval");
    output.println("   make-corpus");
    output.println("   merge-index");
    output.println("   pagerank");
    output.println("   search");
  }

  public void commandHelp(String command) throws IOException {
    if (command.equals("batch-search")) {
      commandHelpBatchSearch();
    } else if (command.equals("build")) {
      commandHelpBuild();
    } else if (command.equals("build-fast")) {
      commandHelpBuild();
    } else if (command.startsWith("pagerank")) {
      PageRankApp.commandHelpPageRank();
    } else if (command.equals("doc")) {
      output.println("galago doc <corpus> <identifier>");
      output.println();
      output.println("  Prints the full text of the document named by <identifier>.");
      output.println("  The document is retrieved from a Corpus file named <corpus>.");
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
    } else if (command.equals("dump-lengths")) {
      output.println("galago dump-lengths <document-lengths-file>");
      output.println();
      output.println("  Dumps all lengths from a length file created by DocumentLengthsWriter.");
      output.println("  This is only for the documentLengths file produced by a Galago indexing job.");
    } else if (command.equals("dump-names")) {
      output.println("galago dump-names <document-names-folder>");
      output.println();
      output.println("  Dumps all names from a names folder created by DocumentNamesWriter2.");
      output.println("  This is only for the documentNames folder produced by a Galago indexing job.");
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
      output.println("                           [default=false]");
      output.println("  --mode={local|threaded|drmaa}: Selects which executor to use ");
      output.println("                           [default=local]");
      output.println("  --galagoTemp=/path/to/temp/dir/: Sets the galago temp dir ");
      output.println("                           [default = uses folders specified in ~/.galagotmp or java.io.tmpdir]");
      output.println("  --deleteOutput={0|1|2}:    Selects how much of the galago temp dir to delete");
      output.println("                           0 --> keep all data");
      output.println("                           1 --> delete all data + keep jobs directory (only useful for drmaa mode)");
      output.println("                           2 --> delete entire temp directory");
      output.println("                           [default=0]");
      output.println("  --distrib={int > 1}:     Selects the number of simultaneous jobs to create");
      output.println("                           [default = 10]");
    } else if (command.equals("merge-index")) {
      output.println("galago merge-index [<flags>+] <output> (<input>)+");
      output.println();
      output.println("  Merges 2 or more indexes. Assumes that the document numberings");
      output.println("  are non-unique. So all documents are assigned new internal numbers.");
      output.println();
      output.println("<output>:  Directory to be created that contains the merged index");
      output.println();
      output.println("<input>:  Directory containing an index to be merged ");
      output.println();
      output.println("  --printJob={true|false}: Simply prints the execution plan of a Tupleflow-based job then exits.");
      output.println("                           [default=false]");
      output.println("  --mode={local|threaded|drmaa}: Selects which executor to use ");
      output.println("                           [default=local]");
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
    } else if (command.equals("doc")) {
      handleDoc(args);
    } else if (command.equals("dump-connection")) {
      handleDumpConnection(args);
    } else if (command.equals("dump-corpus")) {
      handleDumpCorpus(args);
    } else if (command.equals("dump-index")) {
      handleDumpIndex(args);
    } else if (command.equals("dump-keys")) {
      handleDumpKeys(args);
    } else if (command.equals("dump-lengths")) {
      handleDumpLengths(args);
    } else if (command.equals("dump-names")) {
      handleDumpNames(args);
    } else if (command.equals("make-corpus")) {
      handleMakeCorpus(args);
    } else if (command.equals("merge-index")) {
      handleMergeIndexes(args);
    } else if (command.equals("pagerank")) {
      PageRankApp.main(args);
    } else if (command.equals("search")) {
      handleSearch(args);
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
