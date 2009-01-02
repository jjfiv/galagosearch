// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.tools;

import java.io.IOException;
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
    private static void dumpIndex(String[] args) throws IOException {
        StructuredIndexPartReader reader = StructuredIndex.openIndexPart(args[1]);
        IndexIterator iterator = reader.getIterator();
        do {
            System.out.println(iterator.getRecordString());
        } while (iterator.nextRecord());
    }

    private static void handleBuild(String[] args) throws Exception {
        BuildIndex build = new BuildIndex(args[1]);
        Job job = build.getIndexJob(args[1], args[2]);
        ErrorStore store = new ErrorStore();
        JobExecutor.runLocally(job, store);
        if (store.hasStatements()) {
            System.out.println(store.toString());
        }
    }

    private static void handleDoc(String[] args) throws IOException {
        DocumentIndexReader reader = new DocumentIndexReader(args[1]);
        Document document = reader.getDocument(args[2]);
        System.out.println(document.text);
    }

    private static void handleDocs(String[] args) throws IOException {
        DocumentIndexReader reader = new DocumentIndexReader(args[1]);
        DocumentIndexReader.Iterator iterator = reader.getIterator();
        while (!iterator.isDone()) {
            System.out.println(iterator.getKey());
            iterator.getDocument();
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

    private static void handleKeys(String[] args) throws IOException {
        IndexReader reader = new IndexReader(args[1]);
        IndexReader.Iterator iterator = reader.getIterator();
        while (!iterator.isDone()) {
            System.out.println(iterator.getKey());
            iterator.getValueString();
            iterator.nextKey();
        }
    }

    private static void handleMakeCorpus(String[] args) throws Exception {
        Job job = getDocumentConverter(args[1], args[2]);
        ErrorStore store = new ErrorStore();
        JobExecutor.runLocally(job, store);
        if (store.hasStatements()) {
            System.out.println(store.toString());
        }
    }

    private static void handleSearch(String[] args) throws Exception, IOException {
        Retrieval retrieval = Retrieval.instance(args[1]);
        DocumentStore store = null;
        if (args.length > 2) {
            DocumentIndexReader docReader = new DocumentIndexReader(args[2]);
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

    public static Job getDocumentConverter(String directory, String outputCorpus) {
        Job job = new Job();

        Stage stage = new Stage("split");
        stage.add(new StageConnectionPoint(ConnectionPointType.Output, "docs",
                new KeyValuePair.KeyOrder()));
        Parameters p = new Parameters();
        p.add("directory", directory);
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
        System.out.println("Commands:");
        System.out.println("   doc");
        System.out.println("   docs");
        System.out.println("   make-corpus");
        System.out.println("   keys");
        System.out.println("   build");
        System.out.println("   search");
        System.out.println("   dumpconnection");
        System.out.println("   dumpindex");
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException, Exception {
        //args = new String[] { "convert", "/tmp/ws/en/articles", "/tmp/wiki-small.corpus" };
        //args = new String[] { "doc", "/tmp/cacm.corpus", "CACM-3200" };
        //args = new String[] { "docs", "/Users/trevor/Desktop/wiki-small.corpus" };
        //args = new String[] { "build", "/Users/trevor/Desktop/wiki-small.corpus", "/tmp/wiki-small-index" };
        //args = new String[] { "search", "/tmp/wiki-small-index", "/Users/trevor/Desktop/wiki-small.corpus" };

        if (args[0].equals("doc")) {
            handleDoc(args);
        } else if (args[0].equals("make-corpus")) {
            handleMakeCorpus(args);
        } else if (args[0].equals("keys")) {
            handleKeys(args);
        } else if (args[0].equals("docs")) {
            handleDocs(args);
        } else if (args[0].equals("build")) {
            handleBuild(args);
        } else if (args[0].equals("search")) {
            handleSearch(args);
        } else if (args[0].equals("dump-connection")) {
            handleDumpConnection(args);
        } else if (args[0].equals("dump-index")) {
            dumpIndex(args);
        } else {
            usage();
        }
    }
}
