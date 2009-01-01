// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import org.galagosearch.core.index.DocumentLengthsWriter;
import org.galagosearch.core.index.DocumentNameWriter;
import org.galagosearch.core.index.ExtentIndexWriter;
import org.galagosearch.core.index.ExtentValueIndexWriter;
import org.galagosearch.core.index.ManifestWriter;
import org.galagosearch.core.index.PositionIndexWriter;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.index.StructuredIndexPartReader;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.parse.DocumentDataExtractor;
import org.galagosearch.core.parse.DocumentDataNumberer;
import org.galagosearch.core.parse.DocumentIndexReader;
import org.galagosearch.core.parse.DocumentIndexWriter;
import org.galagosearch.core.parse.DocumentSource;
import org.galagosearch.core.parse.DocumentToKeyValuePair;
import org.galagosearch.core.parse.ExtentExtractor;
import org.galagosearch.core.parse.ExtentsNumberer;
import org.galagosearch.core.parse.KeyValuePairToDocument;
import org.galagosearch.core.parse.LinkExtractor;
import org.galagosearch.core.parse.PositionPostingsNumberer;
import org.galagosearch.core.parse.PostingsPositionExtractor;
import org.galagosearch.core.parse.TagTokenizer;
import org.galagosearch.core.parse.UniversalParser;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.tools.Search;
import org.galagosearch.core.tools.SearchWebHandler;
import org.galagosearch.core.retrieval.structured.IndexIterator;
import org.galagosearch.core.store.DocumentIndexStore;
import org.galagosearch.core.store.DocumentStore;
import org.galagosearch.core.store.NullStore;
import org.galagosearch.core.types.DocumentData;
import org.galagosearch.core.types.DocumentExtent;
import org.galagosearch.core.types.NumberedExtent;
import org.galagosearch.tupleflow.Order;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Sorter;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ConnectionPointType;
import org.galagosearch.tupleflow.execution.InputStep;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.execution.MultiStep;
import org.galagosearch.tupleflow.execution.OutputStep;
import org.galagosearch.tupleflow.execution.Stage;
import org.galagosearch.tupleflow.execution.StageConnectionPoint;
import org.galagosearch.tupleflow.execution.Step;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.core.types.DocumentWordPosition;
import org.galagosearch.core.types.ExtractedLink;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.core.types.NumberWordPosition;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.core.types.NumberedValuedExtent;
import org.galagosearch.tupleflow.FileOrderedReader;
import org.galagosearch.tupleflow.types.XMLFragment;
import org.galagosearch.tupleflow.NullSource;
import org.galagosearch.tupleflow.execution.ConnectionAssignmentType;
import org.galagosearch.tupleflow.execution.ErrorStore;
import org.galagosearch.tupleflow.execution.JobExecutor;
import org.mortbay.jetty.Server;

/**
 *
 * @author trevor
 */
public class App {
    String indexPath;

    public Stage getSplitStage(String fileName) {
        Stage stage = new Stage("inputSplit");
        stage.add(new StageConnectionPoint(ConnectionPointType.Output, "splits",
                                           new DocumentSplit.FileNameStartKeyOrder()));
        
        Parameters p = new Parameters();
        p.add("filename", fileName);
        stage.add(new Step(DocumentSource.class, p));
        stage.add(getSorter(new DocumentSplit.FileNameStartKeyOrder()));
        stage.add(new OutputStep("splits"));
        return stage;
    }
    
    public static Step getSorter(Order sortOrder) {
        Parameters p = new Parameters();
        p.add("class", sortOrder.getOrderedClass().getName());
        p.add("order", Utility.join(sortOrder.getOrderSpec()));
        return new Step(Sorter.class, p);
    }
    
    public ArrayList<Step> getExtractionSteps(
            String outputName,
            Class extractionClass,
            Order sortOrder) {
        ArrayList<Step> steps = new ArrayList<Step>();
        steps.add(new Step(extractionClass));
        steps.add(getSorter(sortOrder));
        steps.add(new OutputStep(outputName));
        return steps;
    }
            
    public Stage getParsePostingsStage() {
        Stage stage = new Stage("parsePostings");
        
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Input,
                "splits", new DocumentSplit.FileNameStartKeyOrder()));
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Output,
                "postings", new DocumentWordPosition.DocumentWordPositionOrder()));
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Output,
                "extents", new DocumentExtent.IdentifierOrder()));
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Output,
                "documentData", new DocumentData.IdentifierOrder()));

        stage.add(new InputStep("splits"));
        stage.add(new Step(UniversalParser.class));
        stage.add(new Step(TagTokenizer.class));

        MultiStep multi = new MultiStep();
        ArrayList<Step> text =
                getExtractionSteps("postings", PostingsPositionExtractor.class,
                                   new DocumentWordPosition.DocumentWordPositionOrder());
        ArrayList<Step> extents = 
                getExtractionSteps("extents", ExtentExtractor.class,
                                   new DocumentExtent.IdentifierOrder());
        ArrayList<Step> documentData =
                getExtractionSteps("documentData", DocumentDataExtractor.class,
                                   new DocumentData.IdentifierOrder());

        multi.groups.add(text);
        multi.groups.add(extents);
        multi.groups.add(documentData);
        stage.add(multi);
        return stage;
    }

    public Stage getParseLinksStage() {
        Stage stage = new Stage("parseLinks");
        
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Input,
                "splits", new DocumentSplit.FileNameStartKeyOrder()));
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Output,
                "links", new ExtractedLink.DestUrlOrder()));
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Output,
                "documentData", new DocumentData.UrlOrder()));
        
        stage.add(new InputStep("splits"));
        stage.add(new Step(UniversalParser.class));
        stage.add(new Step(TagTokenizer.class));
        
        MultiStep multi = new MultiStep();
        ArrayList<Step> links =
                getExtractionSteps("links", LinkExtractor.class, new ExtractedLink.DestUrlOrder());
        ArrayList<Step> data =
                getExtractionSteps("documentData", DocumentDataExtractor.class, new DocumentData.UrlOrder());

        multi.groups.add(links);
        multi.groups.add(data);
        stage.add(multi);
                
        return stage;
    }
    
    public Stage getLinkCombineStage() {
        Stage stage = new Stage("linkCombine");
        
        // FIXME: linkCombine stage
        /*
        Parameters p = new Parameters();
        p.add("documentNames", "documentURLs");
        p.add("extractedLinks", "extractedLinks");
        stage.add(new Step(LinkCombiner.class, p));
        stage.add(new Step(AnchorTextDocumentCreator.class));
        
        MultiStep multi = new MultiStep();
        stage.add(multi); */
        
        return stage;
    }
    
    public Stage getWritePostingsStage() {
        Stage stage = new Stage("writePostings");

        stage.add(new StageConnectionPoint(
                ConnectionPointType.Input, "numberedPostings",
                new NumberWordPosition.WordDocumentPositionOrder()));
        stage.add(new InputStep("numberedPostings"));
        Parameters p = new Parameters();
        p.add("filename", indexPath + File.separator + "parts" + File.separator + "postings");
        stage.add(new Step(PositionIndexWriter.class, p));
        return stage;
    }
    
    public Stage getWriteExtentsStage() {
        Stage stage = new Stage("writeExtents");

        stage.add(new StageConnectionPoint(
                ConnectionPointType.Input, "numberedExtents",
                new NumberedExtent.ExtentNameNumberBeginOrder()));

        stage.add(new InputStep("numberedExtents"));
        Parameters p = new Parameters();
        p.add("filename", indexPath + File.separator + "parts" + File.separator + "extents");
        stage.add(new Step(ExtentIndexWriter.class, p));
        return stage;
    }

    public Stage getWriteDatesStage() {
        Stage stage = new Stage("writeDates");

        stage.add(new StageConnectionPoint(
                ConnectionPointType.Input, "numberedDateExtents",
                new NumberedValuedExtent.ExtentNameNumberBeginOrder()));
        Parameters p = new Parameters();
        p.add("filename", indexPath + File.separator + "parts" + File.separator + "dates");
        stage.add(new Step(ExtentValueIndexWriter.class));

        return stage;
    }

    /**
     * For right now, this just dumps out an empty XML file.
     */
    public Stage getWriteManifestStage() {
        // FIXME: Eventually need to add in collection length information to the manifest,
        //   along with stemmer data, etc.
        Stage stage = new Stage("writeManifest");
        
        Parameters p = new Parameters();
        p.add("class", XMLFragment.class.getName());
        stage.add(new Step(NullSource.class, p));
        p = new Parameters();
        p.add("filename", indexPath + File.separator + "manifest");
        stage.add(new Step(ManifestWriter.class, p));
        return stage;
    }

    /**
     * Writes document lengths to a document lengths file.
     */
    public Stage getWriteDocumentLengthsStage() {
        Stage stage = new Stage("writeDocumentLengths");

        stage.add(new StageConnectionPoint(ConnectionPointType.Input,
                "numberedDocumentData", new NumberedDocumentData.NumberOrder()));
        Parameters p = new Parameters();
        p.add("filename", indexPath + File.separator + "documentLengths");
        stage.add(new InputStep("numberedDocumentData"));
        stage.add(new Step(DocumentLengthsWriter.class, p));
        
        return stage;
    }

    /**
     * Writes document names to a document names file.
     */
    public Stage getWriteDocumentNamesStage() {
        Stage stage = new Stage("writeDocumentNames");

        stage.add(new StageConnectionPoint(ConnectionPointType.Input,
                "numberedDocumentData", new NumberedDocumentData.NumberOrder()));
        Parameters p = new Parameters();
        p.add("filename", indexPath + File.separator + "documentNames");
        stage.add(new InputStep("numberedDocumentData"));
        stage.add(new Step(DocumentNameWriter.class, p));
        return stage;
    }

    public Stage getNumberDocumentsStage() {
        Stage stage = new Stage("numberDocuments");

        stage.add(new StageConnectionPoint(ConnectionPointType.Input, "documentData",
                    new DocumentData.IdentifierOrder()));
        stage.add(new StageConnectionPoint(ConnectionPointType.Output, "numberedDocumentData",
                    new NumberedDocumentData.NumberOrder()));
        stage.add(new InputStep("documentData"));
        stage.add(new Step(DocumentDataNumberer.class));
        stage.add(getSorter(new NumberedDocumentData.NumberOrder()));
        stage.add(new OutputStep("numberedDocumentData"));

        return stage;
    }

    public Stage getNumberPostingsStage() {
        Stage stage = new Stage("numberPostings");

        stage.add(new StageConnectionPoint(
                ConnectionPointType.Input,
                "postings", new DocumentWordPosition.DocumentWordPositionOrder()));
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Input,
                "numberedDocumentData", new NumberedDocumentData.NumberOrder()));
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Output,
                "numberedPostings", new NumberWordPosition.WordDocumentPositionOrder()));

        stage.add(new InputStep("postings"));
        stage.add(new Step(PositionPostingsNumberer.class));
        stage.add(getSorter(new NumberWordPosition.WordDocumentPositionOrder()));
        stage.add(new OutputStep("numberedPostings"));

        return stage;
    }

    public Stage getNumberExtentsStage() {
        Stage stage = new Stage("numberExtents");

        stage.add(new StageConnectionPoint(
                ConnectionPointType.Input,
                "extents", new DocumentExtent.IdentifierOrder()));
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Input,
                "numberedDocumentData", new NumberedDocumentData.NumberOrder()));
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Output,
                "numberedExtents", new NumberedExtent.ExtentNameNumberBeginOrder()));

        stage.add(new InputStep("extents"));
        stage.add(new Step(ExtentsNumberer.class));
        stage.add(getSorter(new NumberedExtent.ExtentNameNumberBeginOrder()));
        stage.add(new OutputStep("numberedExtents"));

        return stage;
    }

    public Job getIndexJob(String inputName, String indexDirectory) {
        Job job = new Job();
        this.indexPath = indexDirectory;
        
        job.add(getSplitStage(inputName));
        job.add(getParsePostingsStage());
        job.add(getWritePostingsStage());
        job.add(getWriteManifestStage());
        job.add(getWriteExtentsStage());
        job.add(getWriteDocumentNamesStage());
        job.add(getWriteDocumentLengthsStage());
        job.add(getNumberDocumentsStage());
        job.add(getNumberPostingsStage());
        job.add(getNumberExtentsStage());

        job.connect("inputSplit", "parsePostings", ConnectionAssignmentType.Each);
        job.connect("parsePostings", "numberDocuments", ConnectionAssignmentType.Combined);
        job.connect("numberDocuments", "writeDocumentLengths", ConnectionAssignmentType.Combined);
        job.connect("numberDocuments", "writeDocumentNames", ConnectionAssignmentType.Combined);
        job.connect("numberDocuments", "numberPostings", ConnectionAssignmentType.Combined);
        job.connect("numberDocuments", "numberExtents", ConnectionAssignmentType.Combined);
        job.connect("parsePostings", "numberPostings", ConnectionAssignmentType.Each);
        job.connect("parsePostings", "numberExtents", ConnectionAssignmentType.Each);
        job.connect("numberExtents", "writeExtents", ConnectionAssignmentType.Combined);
        job.connect("numberPostings", "writePostings", ConnectionAssignmentType.Combined);

        return job;
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
        stage.add(getSorter(new KeyValuePair.KeyOrder()));
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
    
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException, Exception {
        //args = new String[] { "convert", "/tmp/ws/en/articles", "/tmp/wiki-small.corpus" };
        //args = new String[] { "doc", "/tmp/cacm.corpus", "CACM-3200" };
        //args = new String[] { "docs", "/Users/trevor/Desktop/wiki-small.corpus" };
        //args = new String[] { "build", "/Users/trevor/Desktop/wiki-small.corpus", "/tmp/wiki-small-index" };
        args =  new String[] { "search", "/tmp/wiki-small-index", "/Users/trevor/Desktop/wiki-small.corpus" };

        if (args[0].equals("doc")) {
            DocumentIndexReader reader = new DocumentIndexReader(args[1]);
            Document document = reader.getDocument(args[2]);
            System.out.println(document.text);
            return;
        }

        if (args[0].equals("convert")) {
            Job job = getDocumentConverter(args[1], args[2]);
            ErrorStore store = new ErrorStore();
            JobExecutor.runLocally(job, store);
            if (store.hasStatements())
                System.out.println(store.toString());
        }

        if (args[0].equals("keys")) {
            IndexReader reader = new IndexReader(args[1]);
            IndexReader.Iterator iterator = reader.getIterator();
            while (!iterator.isDone()) {
                System.out.println(iterator.getKey());
                iterator.getValueString();
                iterator.nextKey();
            }
        }

        if (args[0].equals("docs")) {
            DocumentIndexReader reader = new DocumentIndexReader(args[1]);
            DocumentIndexReader.Iterator iterator = reader.getIterator();
            while (!iterator.isDone()) {
                System.out.println(iterator.getKey());
                iterator.getDocument();
                iterator.nextDocument();
            }
        }

        if (args[0].equals("build")) {
            App app = new App();
            Job job = app.getIndexJob(args[1], args[2]);
            ErrorStore store = new ErrorStore();
            JobExecutor.runLocally(job, store);
            if (store.hasStatements())
                System.out.println(store.toString());
        }

        if (args[0].equals("search")) {
            Retrieval retrieval = Retrieval.instance(args[1]);
            DocumentStore store = null;
            if (args.length > 2) {
                DocumentIndexReader docReader = new DocumentIndexReader(args[2]);
                store = new DocumentIndexStore(docReader);
            } else {
                store = new NullStore();
            }
            Search search = new Search(retrieval, store);
            Server server = new Server(8090);
            server.addHandler(new SearchWebHandler(search));
            server.start();
        }

        if (args[0].equals("dumplist")) {
            FileOrderedReader reader = new FileOrderedReader(args[1]);
            Object o;

            while ((o = reader.read()) != null) {
                System.out.println(o);
            }
        }

        if (args[0].equals("dumpindex")) {
            StructuredIndexPartReader reader = StructuredIndex.openIndexPart(args[1]);
            IndexIterator iterator = reader.getIterator();

            do {
                System.out.println(iterator.getRecordString());
            }
            while (iterator.nextRecord());
        }
    }
}
