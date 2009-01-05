// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
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
    private static void commandHelpBatchSearch() {
        System.out.println("galago batch-search <args>");
        System.out.println();
        System.out.println("  Runs a batch of queries against an index and produces TREC-formatted");
        System.out.println("  output.  The output can be used with retrieval evaluation tools like");
        System.out.println("  galago eval (org.galagosearch.core.eval).");
        System.out.println();
        System.out.println("  Sample invocation:");
        System.out.println("     galago batch-search --index=/tmp/myindex --count=200 /tmp/queries");
        System.out.println();
        System.out.println("  Args:");
        System.out.println("     --index=path_to_your_index");
        System.out.println("     --count : Number of results to return for each query, default=1000");
        System.out.println();
        System.out.println("  Query file format:");
        System.out.println("    The query file is an XML file containing a set of queries.  Each query");
        System.out.println("    has text tag, which contains the text of the query, and a number tag, ");
        System.out.println("    which uniquely identifies the query in the output.");
        System.out.println();
        System.out.println("  Example query file:");
        System.out.println("  <parameters>");
        System.out.println("     <query>");
        System.out.println("        <number>CACM-408</number>");
        System.out.println("        <text>#combine(my query)</text>");
        System.out.println("     </query>");
        System.out.println("     <query>");
        System.out.println("        <number>WIKI-410</number>");
        System.out.println("        <text>#combine(another query)</text>");
        System.out.println("     </query>");
        System.out.println("  </parameters>");
    }

    private static void commandHelpBuild() {
        System.out.println("galago build [flags] <index> (<input>)+");
        System.out.println();
        System.out.println("  Builds a Galago StructuredIndex with TupleFlow, using one thread ");
        System.out.println("  for each CPU core on your computer.  While some debugging output ");
        System.out.println("  will be displayed on the screen, most of the status information will");
        System.out.println("  appear on a web page.  A URL should appear in the command output ");
        System.out.println("  that will direct you to the status page.");
        System.out.println();

        System.out.println("<input>:  Can be either a file or directory, and as many can be");
        System.out.println("          specified as you like.  Galago can read html, xml, txt, ");
        System.out.println("          arc (Heritrix), trectext, trecweb and corpus files.");
        System.out.println("          Files may be gzip compressed (.gz).");
        System.out.println("<index>:  The directory path of the index to produce.");
        System.out.println();
        System.out.println("Flags:");
        System.out.println("  --links={true|false}:    Selects whether to collect anchor text ");
        System.out.println("                           [default=false]");
        System.out.println("  --stemming={true|false}: Selects whether to build stemmed inverted ");
        System.out.println("                           lists in addition to non-stemmed ones.");
        System.out.println("                           [default=true]");
    }

    private static void handleBuild(String[] args) throws Exception {
        // handle --links and --stemming flags
        ArrayList<String> documentFiles = new ArrayList<String>();
        ArrayList<String> flags = new ArrayList<String>();
        for (String arg : Utility.subarray(args, 2)) {
            if (arg.startsWith("--")) {
                flags.add(arg);
            } else {
                documentFiles.add(arg);
            }
        }

        Parameters p = new Parameters(flags.toArray(new String[0]));
        boolean useLinks = p.get("links", false);
        boolean stemming = p.get("stemming", true);
        String[] docs = documentFiles.toArray(new String[0]);

        BuildIndex build = new BuildIndex();
        Job job = build.getIndexJob(args[1], docs, useLinks, stemming);
        ErrorStore store = new ErrorStore();
        JobExecutor.runLocally(job, store);
        if (store.hasStatements()) {
            System.out.println(store.toString());
        }
    }

    private static void handleDoc(String[] args) throws IOException {
        String indexPath = args[1];
        String identifier = args[2];
        DocumentIndexReader reader = new DocumentIndexReader(indexPath);
        Document document = reader.getDocument(identifier);
        System.out.println(document.text);
    }

    private static void handleDumpIndex(String[] args) throws IOException {
        StructuredIndexPartReader reader = StructuredIndex.openIndexPart(args[1]);
        IndexIterator iterator = reader.getIterator();
        do {
            System.out.println(iterator.getRecordString());
        } while (iterator.nextRecord());
    }

    private static void handleDumpCorpus(String[] args) throws IOException {
        DocumentIndexReader reader = new DocumentIndexReader(args[1]);
        DocumentIndexReader.Iterator iterator = reader.getIterator();
        while (!iterator.isDone()) {
            System.out.println("#IDENTIFIER: " + iterator.getKey());
            Document document = iterator.getDocument();
            System.out.println("#METADATA");
            for (Entry<String, String> entry : document.metadata.entrySet()) {
                System.out.println(entry.getKey() + "," + entry.getValue());
            }
            System.out.println("#TEXT");
            System.out.println(document.text);
            iterator.nextDocument();
        }
    }

    private static void handleDumpConnection(String[] args) throws IOException {
        FileOrderedReader reader = new FileOrderedReader(args[1]);
        Object o;
        while ((o = reader.read()) != null) {
            System.out.println(o);
        }
    }

    private static void handleDumpKeys(String[] args) throws IOException {
        IndexReader reader = new IndexReader(args[1]);
        IndexReader.Iterator iterator = reader.getIterator();
        while (!iterator.isDone()) {
            System.out.println(iterator.getKey());
            iterator.getValueString();
            iterator.nextKey();
        }
    }

    private static void handleMakeCorpus(String[] args) throws Exception {
        Job job = getDocumentConverter(args[1], Utility.subarray(args, 2));
        ErrorStore store = new ErrorStore();
        JobExecutor.runLocally(job, store);
        if (store.hasStatements()) {
            System.out.println(store.toString());
        }
    }

    private static void handleBatchSearch(String[] args) throws Exception {
        BatchSearch.main(Utility.subarray(args, 1));
    }

    private static void handleSearch(String[] args) throws Exception, IOException {
        String indexPath = args[1];
        String corpusPath = args[2];

        Retrieval retrieval = Retrieval.instance(indexPath);
        DocumentStore store = null;
        if (args.length > 2) {
            DocumentIndexReader docReader = new DocumentIndexReader(corpusPath);
            store = new DocumentIndexStore(docReader);
        } else {
            store = new NullStore();
        }
        Search search = new Search(retrieval, store);
        int port = Utility.getFreePort();
        Server server = new Server(port);
        server.addHandler(new SearchWebHandler(search));
        server.start();
        System.out.println("Server: http://localhost:" + port);
    }

    public static void handleEval(String[] args) throws IOException {
        org.galagosearch.core.eval.Main.main(args);
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

    public static void usage() {
        System.out.println("Type 'galago help <command>' to get more help about any command,");
        System.out.println("   or 'galago help all' to see all the documentation at once.");
        System.out.println();
        
        System.out.println("Popular commands:");
        System.out.println("   build");
        System.out.println("   search");
        System.out.println("   batch-search");
        System.out.println();

        System.out.println("All commands:");
        System.out.println("   batch-search");
        System.out.println("   build");
        System.out.println("   doc");
        System.out.println("   dump-connection");
        System.out.println("   dump-corpus");
        System.out.println("   dump-index");
        System.out.println("   dump-keys");
        System.out.println("   eval");
        System.out.println("   make-corpus");
        System.out.println("   search");
    }

    public static void commandHelp(String command) throws IOException {
        if (command.equals("batch-search")) {
            commandHelpBatchSearch();
        } else if (command.equals("build")) {
            commandHelpBuild();
        } else if (command.equals("doc")) {
            System.out.println("galago doc <corpus> <identifier>");
            System.out.println();
            System.out.println("  Prints the full text of the document named by <identifier>.");
            System.out.println("  The document is retrieved from a Corpus file named <corpus>.");
        } else if (command.equals("dump-connection")) {
            System.out.println("galago dump-connection <connection-file>");
            System.out.println();
            System.out.println("  Dumps tuples from a Galago TupleFlow connection file in ");
            System.out.println("  CSV format.  This can be useful for debugging strange problems ");
            System.out.println("  in a TupleFlow execution.");
        } else if (command.equals("dump-corpus")) {
            System.out.println("galago dump-corpus <corpus>");
            System.out.println();
            System.out.println("  Dumps all documents from a corpus file to stdout.");
        } else if (command.equals("dump-index")) {
            System.out.println("galago dump-index <index-part>");
            System.out.println();
            System.out.println("  Dumps inverted list data from any index file in a StructuredIndex");
            System.out.println("  (That is, any index that has a readerClass that's a subclass of ");
            System.out.println("  StructuredIndexPartReader).  Output is in CSV format.");
        } else if (command.equals("dump-keys")) {
            System.out.println("galago dump-keys <indexwriter-file>");
            System.out.println();
            System.out.println("  Dumps all keys from any file created by IndexWriter.  This includes");
            System.out.println("  corpus files and all index files built by Galago.");
        } else if (command.equals("eval")) {
            org.galagosearch.core.eval.Main.main(new String[] {});
        } else if (command.equals("make-corpus")) {
            System.out.println("galago make-corpus <corpus> (<input>)+");
            System.out.println();
            System.out.println("  Copies documents from input files into a corpus file.  A corpus");
            System.out.println("  file is required to use any of the document lookup features in ");
            System.out.println("  Galago, like printing snippets of search results.");
            System.out.println();
            System.out.println("<input>:  Can be either a file or directory, and as many can be");
            System.out.println("          specified as you like.  Galago can read html, xml, txt, ");
            System.out.println("          arc (Heritrix), trectext, trecweb and corpus files.");
            System.out.println("          Files may be gzip compressed (.gz).");
        } else if (command.equals("search")) {
            System.out.println("galago search <index> <corpus>");
            System.out.println();
            System.out.println("  Starts a web interface for searching an index interactively.");
            System.out.println("  The URL to use in your web browser will appear in the command ");
            System.out.println("  output.  Cancel the process (Control-C) to quit.");
        } else if (command.equals("all")) {
            String[] commands = { "batch-search", "build", "doc", "dump-connection", "dump-corpus",
                                  "dump-index", "dump-keys", "eval", "make-corpus", "search" };
            for (String c : commands) {
                commandHelp(c);
                System.out.println();
            }
        } else {
            usage();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException, Exception {
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
        } else {
            usage();
        }
    }
}
