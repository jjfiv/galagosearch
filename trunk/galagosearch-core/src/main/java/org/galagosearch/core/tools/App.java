// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.tools;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Map.Entry;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.index.StructuredIndexPartReader;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.parse.DocumentIndexReader;
import org.galagosearch.core.parse.DocumentIndexWriter;
import org.galagosearch.core.parse.DocumentSource;
import org.galagosearch.core.parse.DocumentToKeyValuePair;
import org.galagosearch.core.parse.KeyValuePairToDocument;
import org.galagosearch.core.parse.UniversalParser;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.structured.IndexIterator;
import org.galagosearch.core.store.DocumentIndexStore;
import org.galagosearch.core.store.DocumentStore;
import org.galagosearch.core.store.NullStore;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.execution.ConnectionPointType;
import org.galagosearch.tupleflow.execution.InputStep;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.execution.OutputStep;
import org.galagosearch.tupleflow.execution.Stage;
import org.galagosearch.tupleflow.execution.StageConnectionPoint;
import org.galagosearch.tupleflow.execution.Step;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.tupleflow.FileOrderedReader;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ConnectionAssignmentType;
import org.galagosearch.tupleflow.execution.ErrorStore;
import org.galagosearch.tupleflow.execution.JobExecutor;
import org.mortbay.jetty.Server;

/**
 *
 * @author trevor
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
        output.println("galago build [flags] <index> (<input>)+");
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
        output.println("  --stemming={true|false}: Selects whether to build stemmed inverted ");
        output.println("                           lists in addition to non-stemmed ones.");
        output.println("                           [default=true]");
    }

    private void handleBuild(String[] args) throws Exception {
        // Remove 'build' from the command.
        args = Utility.subarray(args, 1);

        if (args.length <= 0) {
            commandHelpBuild();
            return;
        }

        // handle --links and --stemming flags
        String[][] filtered = Utility.filterFlags(args);

        String[] flags = filtered[0];
        String[] nonFlags = filtered[1];
        String indexName = nonFlags[0];
        String[] docs = Utility.subarray(nonFlags, 1);

        Parameters p = new Parameters(flags);
        boolean useLinks = p.get("links", false);
        boolean stemming = p.get("stemming", true);
        boolean keepOutput = p.get("keepOutput", false);

        BuildIndex build = new BuildIndex();
        Job job = build.getIndexJob(indexName, docs, useLinks, stemming);
        ErrorStore store = new ErrorStore();
        JobExecutor.runLocally(job, store, keepOutput);
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
        DocumentIndexReader reader = new DocumentIndexReader(indexPath);
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

        DocumentIndexReader reader = new DocumentIndexReader(args[1]);
        DocumentIndexReader.Iterator iterator = reader.getIterator();
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

    private void handleMakeCorpus(String[] args) throws Exception {
        if (args.length <= 2) {
            commandHelp(args[0]);
            return;
        }

        Job job = getDocumentConverter(args[1], Utility.subarray(args, 2));
        ErrorStore store = new ErrorStore();
        JobExecutor.runLocally(job, store, false);
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
            ArrayList<DocumentIndexReader> readers = new ArrayList<DocumentIndexReader>();
            for (int i = 0; i < corpusFiles.length; ++i) {
                readers.add(new DocumentIndexReader(corpusFiles[i]));
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

    public static Job getDocumentConverter(String outputCorpus, String[] inputs) throws IOException {
        Job job = new Job();

        Stage stage = new Stage("split");
        stage.add(new StageConnectionPoint(ConnectionPointType.Output, "docs",
                new KeyValuePair.KeyOrder()));
        Parameters p = new Parameters();
        for (String input : inputs) {
            File inputFile = new File(input);

            if (inputFile.isFile()) {
                p.add("filename", input);
            } else if (inputFile.isDirectory()) {
                p.add("directory", input);
            } else {
                throw new IOException("Couldn't find file/directory: " + input);
            }
        }

        stage.add(new Step(DocumentSource.class, p));
        p = new Parameters();
        p.add("identifier", "stripped");
        stage.add(new Step(UniversalParser.class, p));
        stage.add(new Step(DocumentToKeyValuePair.class));
        stage.add(Utility.getSorter(new KeyValuePair.KeyOrder()));
        stage.add(new OutputStep("docs"));
        job.add(stage);

        stage = new Stage("docwrite");
        stage.add(new StageConnectionPoint(ConnectionPointType.Input, "docs",
                new KeyValuePair.KeyOrder()));
        stage.add(new InputStep("docs"));
        stage.add(new Step(KeyValuePairToDocument.class));
        p = new Parameters();
        p.add("filename", outputCorpus);
        stage.add(new Step(DocumentIndexWriter.class, p));

        job.add(stage);
        job.connect("split", "docwrite", ConnectionAssignmentType.Combined);
        return job;
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
        output.println("   doc");
        output.println("   dump-connection");
        output.println("   dump-corpus");
        output.println("   dump-index");
        output.println("   dump-keys");
        output.println("   eval");
        output.println("   make-corpus");
        output.println("   search");
    }

    public void commandHelp(String command) throws IOException {
        if (command.equals("batch-search")) {
            commandHelpBatchSearch();
        } else if (command.equals("build")) {
            commandHelpBuild();
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
        } else if (command.equals("eval")) {
            org.galagosearch.core.eval.Main.usage(output);
        } else if (command.equals("make-corpus")) {
            output.println("galago make-corpus <corpus> (<input>)+");
            output.println();
            output.println("  Copies documents from input files into a corpus file.  A corpus");
            output.println("  file is required to use any of the document lookup features in ");
            output.println("  Galago, like printing snippets of search results.");
            output.println();
            output.println("<input>:  Can be either a file or directory, and as many can be");
            output.println("          specified as you like.  Galago can read html, xml, txt, ");
            output.println("          arc (Heritrix), trectext, trecweb and corpus files.");
            output.println("          Files may be gzip compressed (.gz).");
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
            String[] commands = { "batch-search", "build", "doc", "dump-connection", "dump-corpus",
                                  "dump-index", "dump-keys", "eval", "make-corpus", "search" };
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
        } else if (command.equals("make-corpus")) {
            handleMakeCorpus(args);
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
