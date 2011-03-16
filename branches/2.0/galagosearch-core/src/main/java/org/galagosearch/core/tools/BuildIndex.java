// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.galagosearch.core.index.corpus.SplitIndexKeyWriter;
import org.galagosearch.core.index.PositionIndexWriter;
import org.galagosearch.core.index.corpus.CorpusWriter;
import org.galagosearch.core.parse.AdditionalTextCombiner;
import org.galagosearch.core.parse.AnchorTextCreator;
import org.galagosearch.core.parse.DocumentDataExtractor;
import org.galagosearch.core.parse.DocumentDataNumberer;
import org.galagosearch.core.parse.DocumentSource;
import org.galagosearch.core.parse.ExtentExtractor;
import org.galagosearch.core.parse.ExtentsNumberer;
import org.galagosearch.core.parse.LinkCombiner;
import org.galagosearch.core.parse.LinkExtractor;
import org.galagosearch.core.parse.PositionPostingsNumberer;
import org.galagosearch.core.parse.PostingsPositionExtractor;
import org.galagosearch.core.parse.TagTokenizer;
import org.galagosearch.core.parse.UniversalParser;
import org.galagosearch.core.types.AdditionalDocumentText;
import org.galagosearch.core.types.DocumentData;
import org.galagosearch.core.types.DocumentExtent;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.core.types.DocumentWordPosition;
import org.galagosearch.core.types.ExtractedLink;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.core.types.NumberWordPosition;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.core.types.NumberedExtent;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.execution.ConnectionAssignmentType;
import org.galagosearch.tupleflow.execution.InputStep;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.execution.MultiStep;
import org.galagosearch.tupleflow.execution.OutputStep;
import org.galagosearch.tupleflow.execution.Stage;
import org.galagosearch.tupleflow.execution.Step;
import org.galagosearch.tupleflow.types.XMLFragment;

/**
 *
 * @author trevor
 */
public class BuildIndex {

  File indexPath;
  boolean stemming;
  boolean useLinks;
  String indexunit;
  boolean makeCorpus;
  Parameters corpusParameters;
  Parameters buildParameters;

  public BuildIndex() {
    this.stemming = false;
    this.useLinks = false;

  }

  public Stage getParsePostingsStage() {
    Stage stage = new Stage("parsePostings");

    // Connections
    stage.addInput("splits", new DocumentSplit.FileIdOrder());
    stage.addOutput("postings", new DocumentWordPosition.DocumentWordPositionOrder());
    stage.addOutput("extents", new DocumentExtent.IdentifierOrder());
    stage.addOutput("documentData", new DocumentData.IdentifierOrder());
    if (stemming) {
      stage.addOutput("stemmedPostings", new DocumentWordPosition.DocumentWordPositionOrder());
    }
    if (useLinks) {
      stage.addInput("anchorText", new AdditionalDocumentText.IdentifierOrder());
    }
    if (makeCorpus) {
      stage.addOutput("corpusKeyData", new KeyValuePair.KeyOrder());
    }

    // Steps
    stage.add(new InputStep("splits"));

    stage.add(BuildStageTemplates.getParserStep(buildParameters));

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

    indexer.add(BuildStageTemplates.getTokenizerStep(buildParameters));

    // processing now forks into 3 or 4 more threads
    MultiStep processingForkTwo = new MultiStep();
    ArrayList<Step> text =
            BuildStageTemplates.getExtractionSteps("postings", PostingsPositionExtractor.class,
            new DocumentWordPosition.DocumentWordPositionOrder());
    ArrayList<Step> extents =
            BuildStageTemplates.getExtractionSteps("extents", ExtentExtractor.class,
            new DocumentExtent.IdentifierOrder());
    ArrayList<Step> documentData =
            BuildStageTemplates.getExtractionSteps("documentData", DocumentDataExtractor.class,
            new DocumentData.IdentifierOrder());

    // insert each fork into the
    processingForkTwo.groups.add(text);
    processingForkTwo.groups.add(extents);
    processingForkTwo.groups.add(documentData);

    if (stemming) {
      ArrayList<Step> stemmedSteps = new ArrayList<Step>();
      stemmedSteps.add(BuildStageTemplates.getStemmerStep(buildParameters));
      stemmedSteps.add(new Step(PostingsPositionExtractor.class));
      stemmedSteps.add(Utility.getSorter(new DocumentWordPosition.DocumentWordPositionOrder()));
      stemmedSteps.add(new OutputStep("stemmedPostings"));
      processingForkTwo.groups.add(stemmedSteps);
    }

    // main thread is now completely defined
    indexer.add(processingForkTwo);
    // fork one is now complete.
    processingForkOne.groups.add(indexer);
    // add the fork into the stage.
    stage.add(processingForkOne);

    return stage;
  }

  public Stage getParseLinksStage() {
    Stage stage = new Stage("parseLinks");

    // Connections
    stage.addInput("splits", new DocumentSplit.FileIdOrder());
    stage.addOutput("links", new ExtractedLink.DestUrlOrder());
    stage.addOutput("documentUrls", new DocumentData.UrlOrder());

    // Steps
    stage.add(new InputStep("splits"));
    stage.add(new Step(UniversalParser.class));
    stage.add(new Step(TagTokenizer.class));

    MultiStep multi = new MultiStep();
    ArrayList<Step> links =
            BuildStageTemplates.getExtractionSteps("links", LinkExtractor.class, new ExtractedLink.DestUrlOrder());
    ArrayList<Step> data =
            BuildStageTemplates.getExtractionSteps("documentUrls", DocumentDataExtractor.class,
            new DocumentData.UrlOrder());

    multi.groups.add(links);
    multi.groups.add(data);
    stage.add(multi);

    return stage;
  }

  public Stage getLinkCombineStage() {
    Stage stage = new Stage("linkCombine");

    // Connections
    stage.addInput("documentUrls", new DocumentData.UrlOrder());
    stage.addInput("links", new ExtractedLink.DestUrlOrder());
    stage.addOutput("anchorText", new AdditionalDocumentText.IdentifierOrder());

    // Steps
    Parameters p = new Parameters();
    p.add("documentDatas", "documentUrls");
    p.add("extractedLinks", "links");
    stage.add(new Step(LinkCombiner.class, p));
    stage.add(new Step(AnchorTextCreator.class));
    stage.add(Utility.getSorter(new AdditionalDocumentText.IdentifierOrder()));
    stage.add(new OutputStep("anchorText"));

    return stage;
  }

  public Stage getWritePostingsStage(String stageName, String inputName, String indexName) {
    Stage stage = new Stage(stageName);

    stage.addInput(inputName, new NumberWordPosition.WordDocumentPositionOrder());
    stage.addInput("collectionLength", new XMLFragment.NodePathOrder());
    stage.add(new InputStep(inputName));
    Parameters p = new Parameters();
    p.add("filename", indexPath + File.separator + indexName);
    stage.add(new Step(PositionIndexWriter.class, p));
    return stage;
  }

  public Stage getParallelIndexKeyWriterStage(String name, String input, Parameters indexParameters) {
    Stage stage = new Stage(name);

    stage.addInput(input, new KeyValuePair.KeyOrder());

    // Steps
    stage.add(new InputStep(input));
    stage.add(new Step(SplitIndexKeyWriter.class, indexParameters));

    return stage;
  }

  public Stage getNumberDocumentsStage() {
    Stage stage = new Stage("numberDocuments");

    // Connections
    stage.addInput("documentData", new DocumentData.IdentifierOrder());
    stage.addOutput("numberedDocumentData", new NumberedDocumentData.NumberOrder());

    // Steps
    stage.add(new InputStep("documentData"));
    stage.add(new Step(DocumentDataNumberer.class));
    stage.add(Utility.getSorter(new NumberedDocumentData.NumberOrder()));
    stage.add(new OutputStep("numberedDocumentData"));

    return stage;
  }

  public Stage getNumberPostingsStage(String stageName, String inputName, String outputName) {
    Stage stage = new Stage(stageName);

    // Connections
    stage.addInput(inputName, new DocumentWordPosition.DocumentWordPositionOrder());
    stage.addInput("numberedDocumentData", new NumberedDocumentData.NumberOrder());
    stage.addOutput(outputName, new NumberWordPosition.WordDocumentPositionOrder());

    // Steps
    stage.add(new InputStep(inputName));
    stage.add(new Step(PositionPostingsNumberer.class));
    stage.add(Utility.getSorter(new NumberWordPosition.WordDocumentPositionOrder()));
    stage.add(new OutputStep(outputName));

    return stage;
  }

  public Stage getNumberExtentsStage() {
    Stage stage = new Stage("numberExtents");

    // Connections
    stage.addInput("extents", new DocumentExtent.IdentifierOrder());
    stage.addInput("numberedDocumentData", new NumberedDocumentData.NumberOrder());
    stage.addOutput("numberedExtents", new NumberedExtent.ExtentNameNumberBeginOrder());

    // Steps
    stage.add(new InputStep("extents"));
    stage.add(new Step(ExtentsNumberer.class));
    stage.add(Utility.getSorter(new NumberedExtent.ExtentNameNumberBeginOrder()));
    stage.add(new OutputStep("numberedExtents"));

    return stage;
  }

  public Job getIndexJob(Parameters p) throws IOException {

    Job job = new Job();
    this.buildParameters = p;
    this.stemming = p.get("stemming", true);
    this.useLinks = p.get("links", false);
    this.indexPath = new File(p.get("indexPath")); // fail if no path.
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
      this.corpusParameters.add("parallel", "true");
      this.corpusParameters.add("compressed", p.get("compressed", "true"));
      this.corpusParameters.add("filename", new File(p.get("corpusPath")).getAbsolutePath());
    }

    job.add(BuildStageTemplates.getSplitStage(inputPaths, DocumentSource.class));
    job.add(getParsePostingsStage());
    job.add(getWritePostingsStage("writePostings", "numberedPostings", "postings"));
    //job.add(BuildStageTemplates.getWriteManifestStage("writeManifest", new File(indexPath, "manifest"), "collectionLength", "postings"));
    job.add(BuildStageTemplates.getWriteExtentsStage("writeExtents", new File(indexPath, "extents"), "numberedExtents"));
    job.add(BuildStageTemplates.getWriteNamesStage("writeNames", new File(indexPath, "names"), "numberedDocumentData"));
    job.add(BuildStageTemplates.getWriteLengthsStage("writeLengths", new File(indexPath, "lengths"), "numberedDocumentData"));
    job.add(getNumberDocumentsStage());
    job.add(getNumberPostingsStage("numberPostings", "postings", "numberedPostings"));
    job.add(getNumberExtentsStage());
    job.add(BuildStageTemplates.getCollectionLengthStage("collectionLength", "numberedDocumentData", "collectionLength"));

    job.connect("inputSplit", "parsePostings", ConnectionAssignmentType.Each);
    job.connect("parsePostings", "numberDocuments", ConnectionAssignmentType.Combined);
    job.connect("numberDocuments", "collectionLength", ConnectionAssignmentType.Combined);
    job.connect("numberDocuments", "writeLengths", ConnectionAssignmentType.Combined);
    job.connect("numberDocuments", "writeNames", ConnectionAssignmentType.Combined);
    job.connect("numberDocuments", "numberPostings", ConnectionAssignmentType.Combined);
    job.connect("numberDocuments", "numberExtents", ConnectionAssignmentType.Combined);
    job.connect("parsePostings", "numberPostings", ConnectionAssignmentType.Each);
    job.connect("parsePostings", "numberExtents", ConnectionAssignmentType.Each);
    job.connect("numberExtents", "writeExtents", ConnectionAssignmentType.Combined);
    job.connect("numberPostings", "writePostings", ConnectionAssignmentType.Combined);
    job.connect("collectionLength", "writePostings", ConnectionAssignmentType.Combined);

    if (useLinks) {
      job.add(getParseLinksStage());
      job.add(getLinkCombineStage());

      job.connect("inputSplit", "parseLinks", ConnectionAssignmentType.Each);
      job.connect("parseLinks", "linkCombine", ConnectionAssignmentType.Combined); // this should be Each, but the subsequent connection wouldnt work
      job.connect("linkCombine", "parsePostings", ConnectionAssignmentType.Combined);
    }

    if (stemming) {
      job.add(getNumberPostingsStage("numberStemmedPostings",
              "stemmedPostings",
              "numberedStemmedPostings"));
      job.add(getWritePostingsStage("writeStemmedPostings",
              "numberedStemmedPostings",
              "stemmedPostings"));
      job.connect("parsePostings", "numberStemmedPostings", ConnectionAssignmentType.Each);
      job.connect("numberDocuments", "numberStemmedPostings", ConnectionAssignmentType.Combined);
      job.connect("numberStemmedPostings", "writeStemmedPostings", ConnectionAssignmentType.Combined);
      job.connect("collectionLength", "writeStemmedPostings", ConnectionAssignmentType.Combined);
    }

    if (makeCorpus) {
      job.add(getParallelIndexKeyWriterStage("corpusIndexWriter", "corpusKeyData", this.corpusParameters));
      job.connect("parsePostings", "corpusIndexWriter", ConnectionAssignmentType.Combined);
    }

    return job;
  }
}
