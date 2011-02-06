// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.galagosearch.core.index.DocumentLengthsWriter;
import org.galagosearch.core.index.DocumentNameWriter;
import org.galagosearch.core.index.ExtentIndexWriter;
import org.galagosearch.core.index.ExtentValueIndexWriter;
import org.galagosearch.core.index.ManifestWriter;
import org.galagosearch.core.index.parallel.ParallelIndexKeyWriter;
import org.galagosearch.core.index.PositionIndexWriter;
import org.galagosearch.core.parse.AdditionalTextCombiner;
import org.galagosearch.core.parse.AnchorTextCreator;
import org.galagosearch.core.parse.CollectionLengthCounterNDD;
import org.galagosearch.core.index.corpus.CorpusReader;
import org.galagosearch.core.index.corpus.CorpusWriter;
import org.galagosearch.core.index.corpus.DocumentToKeyValuePair;
import org.galagosearch.core.index.parallel.ParallelIndexValueWriter;
import org.galagosearch.core.parse.DocumentDataExtractor;
import org.galagosearch.core.parse.FastDocumentNumberer;
import org.galagosearch.core.parse.DocumentSource;
import org.galagosearch.core.parse.LinkCombiner;
import org.galagosearch.core.parse.LinkExtractor;
import org.galagosearch.core.parse.NumberedDocument;
import org.galagosearch.core.parse.NumberedDocumentDataExtractor;
import org.galagosearch.core.parse.NumberedExtentExtractor;
import org.galagosearch.core.parse.NumberedPostingsPositionExtractor;
import org.galagosearch.core.parse.Porter2Stemmer;
import org.galagosearch.core.parse.TagTokenizer;
import org.galagosearch.core.parse.UniversalParser;
import org.galagosearch.core.types.AdditionalDocumentText;
import org.galagosearch.core.types.DocumentData;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.core.types.ExtractedLink;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.core.types.NumberWordPosition;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.core.types.NumberedExtent;
import org.galagosearch.core.types.NumberedValuedExtent;
import org.galagosearch.tupleflow.Order;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.execution.ConnectionAssignmentType;
import org.galagosearch.tupleflow.execution.ConnectionPointType;
import org.galagosearch.tupleflow.execution.InputStep;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.execution.MultiStep;
import org.galagosearch.tupleflow.execution.OutputStep;
import org.galagosearch.tupleflow.execution.Stage;
import org.galagosearch.tupleflow.execution.StageConnectionPoint;
import org.galagosearch.tupleflow.execution.Step;
import org.galagosearch.tupleflow.types.XMLFragment;

/**
 *
 * Builds an index using a faster method 
 * (it requires one less sort of the posting lists)
 *
 * @author sjh
 */
public class BuildFastIndex {

    protected String indexPath;
    protected boolean stemming;
    protected boolean useLinks;
    protected boolean makeCorpus;
    protected Parameters corpusParameters;

    public Stage getSplitStage(ArrayList<String> inputPaths) throws IOException {
        Stage stage = new Stage("inputSplit");
        stage.add(new StageConnectionPoint(ConnectionPointType.Output, "splits",
                new DocumentSplit.FileIdOrder()));

        Parameters p = new Parameters();
        for (String input : inputPaths) {
            File inputFile = new File(input);

            if (inputFile.isFile()) {
                p.add("filename", inputFile.getAbsolutePath());
            } else if (inputFile.isDirectory()) {
                p.add("directory", inputFile.getAbsolutePath());
            } else {
                throw new IOException("Couldn't find file/directory: " + input);
            }
        }

        stage.add(new Step(DocumentSource.class, p));
        stage.add(Utility.getSorter(new DocumentSplit.FileIdOrder()));
        stage.add(new OutputStep("splits"));
        return stage;
    }

    public ArrayList<Step> getExtractionSteps(
            String outputName,
            Class extractionClass,
            Order sortOrder) {
        ArrayList<Step> steps = new ArrayList<Step>();
        steps.add(new Step(extractionClass));
        steps.add(Utility.getSorter(sortOrder));
        steps.add(new OutputStep(outputName));
        return steps;
    }

    public Stage getParsePostingsStage() {
        Stage stage = new Stage("parsePostings");

        stage.add(new StageConnectionPoint(
                ConnectionPointType.Input,
                "splits", new DocumentSplit.FileIdOrder()));
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Output,
                "numberedPostings", new NumberWordPosition.WordDocumentPositionOrder()));
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Output,
                "numberedExtents", new NumberedExtent.ExtentNameNumberBeginOrder()));
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Output,
                "numberedDocumentData", new NumberedDocumentData.NumberOrder()));
        if (stemming) {
            stage.add(new StageConnectionPoint(
                    ConnectionPointType.Output,
                    "numberedStemmedPostings", new NumberWordPosition.WordDocumentPositionOrder()));
        }
        if (useLinks) {
            stage.add(new StageConnectionPoint(
                    ConnectionPointType.Input,
                    "anchorText", new AdditionalDocumentText.IdentifierOrder()));
        }

        if (makeCorpus) {
            stage.add(new StageConnectionPoint(
                    ConnectionPointType.Output,
                    "corpusKeyData", new KeyValuePair.KeyOrder()));
        }

        stage.add(new InputStep("splits"));
        stage.add(new Step(UniversalParser.class));

        // if we are making a corpus - it needs to be spun off here:
        MultiStep processingForkOne = new MultiStep();

        if (makeCorpus) {
            ArrayList<Step> corpus = new ArrayList();
            corpus.add(new Step(CorpusWriter.class, corpusParameters.clone()));
            corpus.add(Utility.getSorter(new KeyValuePair.KeyOrder()));
            corpus.add(new OutputStep("corpusKeyData"));

            processingForkOne.groups.add(corpus);
        }

        // main processing thread continues with these steps;
        ArrayList<Step> indexer = new ArrayList();

        if (useLinks) {
            Parameters p = new Parameters();
            p.add("textSource", "anchorText");
            indexer.add(new Step(AdditionalTextCombiner.class, p));
        }

        indexer.add(new Step(TagTokenizer.class));
        indexer.add(new Step(FastDocumentNumberer.class));

        MultiStep processingForkTwo = new MultiStep();
        ArrayList<Step> text =
                getExtractionSteps("numberedPostings", NumberedPostingsPositionExtractor.class,
                new NumberWordPosition.WordDocumentPositionOrder());
        ArrayList<Step> extents =
                getExtractionSteps("numberedExtents", NumberedExtentExtractor.class,
                new NumberedExtent.ExtentNameNumberBeginOrder());
        ArrayList<Step> documentData =
                getExtractionSteps("numberedDocumentData", NumberedDocumentDataExtractor.class,
                new NumberedDocumentData.NumberOrder());

        processingForkTwo.groups.add(text);
        processingForkTwo.groups.add(extents);
        processingForkTwo.groups.add(documentData);

        if (stemming) {
            ArrayList<Step> stemmedSteps = new ArrayList<Step>();
            Parameters p = new Parameters();
            p.add("class", NumberedDocument.class.getName());
            stemmedSteps.add(new Step(Porter2Stemmer.class, p));
            stemmedSteps.add(new Step(NumberedPostingsPositionExtractor.class));
            stemmedSteps.add(Utility.getSorter(new NumberWordPosition.WordDocumentPositionOrder()));
            stemmedSteps.add(new OutputStep("numberedStemmedPostings"));
            processingForkTwo.groups.add(stemmedSteps);
        }

        indexer.add(processingForkTwo);
        processingForkOne.groups.add(indexer);
        stage.add(processingForkOne);

        return stage;
    }

    public Stage getParseLinksStage() {
        Stage stage = new Stage("parseLinks");

        stage.add(new StageConnectionPoint(
                ConnectionPointType.Input,
                "splits", new DocumentSplit.FileIdOrder()));
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Output,
                "links", new ExtractedLink.DestUrlOrder()));
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Output,
                "documentUrls", new DocumentData.UrlOrder()));

        stage.add(new InputStep("splits"));
        stage.add(new Step(UniversalParser.class));
        stage.add(new Step(TagTokenizer.class));

        MultiStep multi = new MultiStep();
        ArrayList<Step> links =
                getExtractionSteps("links", LinkExtractor.class, new ExtractedLink.DestUrlOrder());
        ArrayList<Step> data =
                getExtractionSteps("documentUrls", DocumentDataExtractor.class,
                new DocumentData.UrlOrder());

        multi.groups.add(links);
        multi.groups.add(data);
        stage.add(multi);

        return stage;
    }

    public Stage getLinkCombineStage() {
        Stage stage = new Stage("linkCombine");

        stage.add(new StageConnectionPoint(ConnectionPointType.Input, "documentUrls",
                new DocumentData.UrlOrder()));
        stage.add(new StageConnectionPoint(ConnectionPointType.Input, "links",
                new ExtractedLink.DestUrlOrder()));
        stage.add(new StageConnectionPoint(ConnectionPointType.Output, "anchorText",
                new AdditionalDocumentText.IdentifierOrder()));

        Parameters p = new Parameters();
        p.add("documentDatas", "documentUrls");
        p.add("extractedLinks", "links");
        stage.add(new Step(LinkCombiner.class, p));
        stage.add(new Step(AnchorTextCreator.class));
        stage.add(Utility.getSorter(new AdditionalDocumentText.IdentifierOrder()));
        stage.add(new OutputStep("anchorText"));

        return stage;
    }

    public Stage getCollectionLengthStage() {
        Stage stage = new Stage("collectionLength");

        stage.add(new StageConnectionPoint(
                ConnectionPointType.Input, "numberedDocumentData",
                new NumberedDocumentData.NumberOrder()));
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Output, "collectionLength",
                new XMLFragment.NodePathOrder()));

        stage.add(new InputStep("numberedDocumentData"));
        stage.add(new Step(CollectionLengthCounterNDD.class));
        stage.add(Utility.getSorter(new XMLFragment.NodePathOrder()));
        stage.add(new OutputStep("collectionLength"));

        return stage;
    }

    public Stage getWritePostingsStage(String stageName, String inputName, String indexName) {
        Stage stage = new Stage(stageName);

        stage.add(new StageConnectionPoint(
                ConnectionPointType.Input, inputName,
                new NumberWordPosition.WordDocumentPositionOrder()));
        stage.add(new InputStep(inputName));
        Parameters p = new Parameters();
        p.add("filename", indexPath + File.separator + "parts" + File.separator + indexName);
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
     * Write out document count and collection length information.
     */
    public Stage getWriteManifestStage() {
        Stage stage = new Stage("writeManifest");

        stage.add(new StageConnectionPoint(ConnectionPointType.Input,
                "collectionLength",
                new XMLFragment.NodePathOrder()));
        stage.add(new InputStep("collectionLength"));
        Parameters p = new Parameters();
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

    public Stage getParallelIndexKeyWriterStage(String name, String input, Parameters indexParameters) {
        Stage stage = new Stage(name);

        stage.add(new StageConnectionPoint(
                ConnectionPointType.Input, input,
                new KeyValuePair.KeyOrder()));

        stage.add(new InputStep(input));
        stage.add(new Step(ParallelIndexKeyWriter.class, indexParameters));

        return stage;
    }

    public Job getIndexJob(Parameters p) throws IOException {

        Job job = new Job();
        this.stemming = p.get("stemming", true);
        this.useLinks = p.get("links", false);
        this.indexPath = new File(p.get("indexPath")).getAbsolutePath(); // fail if no path.
        this.makeCorpus = p.containsKey("corpusPath");

        ArrayList<String> inputPaths = new ArrayList();
        List<Value> vs = p.list("inputPaths");
        for (Value v : vs) {
            inputPaths.add(v.toString());
        }
        // ensure the index folder exists
        Utility.makeParentDirectories(indexPath);
        if (makeCorpus) {
            this.corpusParameters = new Parameters();
            this.corpusParameters.add("compressed", p.get("compressed", "true"));
            this.corpusParameters.add("filename", new File(p.get("corpusPath")).getAbsolutePath());
            this.corpusParameters.add("readerClass", CorpusReader.class.getName());
            this.corpusParameters.add("writerClass", CorpusWriter.class.getName());
        }

        job.add(getSplitStage(inputPaths));
        job.add(getParsePostingsStage());
        job.add(getWritePostingsStage("writePostings", "numberedPostings", "postings"));
        job.add(getWriteManifestStage());
        job.add(getWriteExtentsStage());
        job.add(getWriteDocumentNamesStage());
        job.add(getWriteDocumentLengthsStage());
        job.add(getCollectionLengthStage());

        job.connect("inputSplit", "parsePostings", ConnectionAssignmentType.Each);
        job.connect("parsePostings", "writeDocumentLengths", ConnectionAssignmentType.Combined);
        job.connect("parsePostings", "writeDocumentNames", ConnectionAssignmentType.Combined);
        job.connect("parsePostings", "writeExtents", ConnectionAssignmentType.Combined);
        job.connect("parsePostings", "writePostings", ConnectionAssignmentType.Combined);
        job.connect("parsePostings", "collectionLength", ConnectionAssignmentType.Combined);
        job.connect("collectionLength", "writeManifest", ConnectionAssignmentType.Combined);

        if (useLinks) {
            job.add(getParseLinksStage());
            job.add(getLinkCombineStage());

            job.connect("inputSplit", "parseLinks", ConnectionAssignmentType.Each);
            job.connect("parseLinks", "linkCombine", ConnectionAssignmentType.Combined); // this should be Each, but the subsequent connection wouldnt work
            job.connect("linkCombine", "parsePostings", ConnectionAssignmentType.Combined);
        }

        if (stemming) {
            job.add(getWritePostingsStage("writeStemmedPostings",
                    "numberedStemmedPostings",
                    "stemmedPostings"));
            job.connect("parsePostings", "writeStemmedPostings", ConnectionAssignmentType.Combined);
        }

        if (makeCorpus) {
            job.add(getParallelIndexKeyWriterStage("corpusIndexWriter", "corpusKeyData", this.corpusParameters));
            job.connect("parsePostings", "corpusIndexWriter", ConnectionAssignmentType.Combined);
        }

        return job;
    }
}
