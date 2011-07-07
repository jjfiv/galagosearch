// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.galagosearch.core.index.corpus.SplitIndexKeyWriter;
import org.galagosearch.core.index.PositionIndexWriter;
import org.galagosearch.core.index.PositionFieldIndexWriter;
import org.galagosearch.core.index.corpus.CorpusReader;
import org.galagosearch.core.parse.AdditionalTextCombiner;
import org.galagosearch.core.parse.AnchorTextCreator;
import org.galagosearch.core.index.corpus.CorpusWriter;
import org.galagosearch.core.parse.DocumentDataExtractor;
import org.galagosearch.core.parse.FastDocumentNumberer;
import org.galagosearch.core.parse.DocumentSource;
import org.galagosearch.core.parse.LinkCombiner;
import org.galagosearch.core.parse.LinkExtractor;
import org.galagosearch.core.parse.NumberedDocument;
import org.galagosearch.core.parse.NumberedDocumentDataExtractor;
import org.galagosearch.core.parse.NumberedExtentExtractor;
import org.galagosearch.core.parse.NumberedExtentPostingsExtractor;
import org.galagosearch.core.parse.NumberedPostingsPositionExtractor;
import org.galagosearch.core.parse.TagTokenizer;
import org.galagosearch.core.parse.UniversalParser;
import org.galagosearch.core.types.AdditionalDocumentText;
import org.galagosearch.core.types.DocumentData;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.core.types.ExtractedLink;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.core.types.FieldNumberWordPosition;
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
 * Builds an index using a faster method 
 * (it requires one less sort of the posting lists)
 *
 * @author sjh
 */
public class BuildFastIndex {

  protected File indexPath;
  protected boolean stemming;
  protected boolean useLinks;
  protected boolean makeCorpus;
  protected Parameters corpusParameters;
  protected Parameters buildParameters;

  public Stage getParsePostingsStage() {
    Stage stage = new Stage("parsePostings");

    // Connections
    stage.addInput("splits", new DocumentSplit.FileIdOrder());
    stage.addOutput("numberedPostings", new NumberWordPosition.WordDocumentPositionOrder());
    stage.addOutput("numberedExtents", new NumberedExtent.ExtentNameNumberBeginOrder());
    stage.addOutput("numberedExtentPostings", new FieldNumberWordPosition.FieldWordDocumentPositionOrder());
    stage.addOutput("numberedDocumentData", new NumberedDocumentData.NumberOrder());
    if (stemming) {
      stage.addOutput("numberedStemmedPostings", new NumberWordPosition.WordDocumentPositionOrder());
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
    indexer.add(new Step(FastDocumentNumberer.class));

    MultiStep processingForkTwo = new MultiStep();
    ArrayList<Step> text =
            BuildStageTemplates.getExtractionSteps("numberedPostings", NumberedPostingsPositionExtractor.class,
            new NumberWordPosition.WordDocumentPositionOrder());
    ArrayList<Step> extents =
            BuildStageTemplates.getExtractionSteps("numberedExtents", NumberedExtentExtractor.class,
            new NumberedExtent.ExtentNameNumberBeginOrder());
    ArrayList<Step> extentPostings =
            BuildStageTemplates.getExtractionSteps("numberedExtentPostings", NumberedExtentPostingsExtractor.class,
            new FieldNumberWordPosition.FieldWordDocumentPositionOrder());
    ArrayList<Step> documentData =
            BuildStageTemplates.getExtractionSteps("numberedDocumentData", NumberedDocumentDataExtractor.class,
            new NumberedDocumentData.NumberOrder());

    processingForkTwo.groups.add(text);
    processingForkTwo.groups.add(extents);
    processingForkTwo.groups.add(extentPostings);
    processingForkTwo.groups.add(documentData);
    if (stemming) {
      ArrayList<Step> stemmedSteps = new ArrayList<Step>();
      Parameters p = new Parameters();
      if (buildParameters.containsKey("stemmer")) {
        p.add("stemmer", buildParameters.list("stemmer"));
      }
      p.add("stemmer/outputClass", NumberedDocument.class.getName());
      stemmedSteps.add(BuildStageTemplates.getStemmerStep(p));
      //stemmedSteps.add(new Step(Porter2Stemmer.class, p));
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
    p.add("pipename", "collectionLength");
    stage.add(new Step(PositionIndexWriter.class, p));
    return stage;
  }

  /** 
   */
  public Stage getWriteExtentPostingsStage(String stageName, String inputName, String indexName) {
    Stage stage = new Stage(stageName);

    stage.addInput(inputName, new FieldNumberWordPosition.FieldWordDocumentPositionOrder());
    // stage.addInput("collectionLength", new XMLFragment.NodePathOrder());
    stage.add(new InputStep(inputName));
    Parameters p = new Parameters();
    p.add("filename", indexPath + File.separator + indexName);
    // p.add("pipename", "collectionLength");
    stage.add(new Step(PositionFieldIndexWriter.class, p));
    return stage;
  }

  public Stage getParallelIndexKeyWriterStage(String name, String input, Parameters indexParameters) {
    Stage stage = new Stage(name);

    stage.addInput(input, new KeyValuePair.KeyOrder());

    stage.add(new InputStep(input));
    stage.add(new Step(SplitIndexKeyWriter.class, indexParameters));

    return stage;
  }

  public Job getIndexJob(Parameters p) throws IOException {

    Job job = new Job();
    this.buildParameters = p;
    this.stemming = p.get("stemming", true);
    this.useLinks = p.get("links", false);
    this.indexPath = new File(p.get("indexPath")).getAbsoluteFile(); // fail if no path.
    this.makeCorpus = p.containsKey("corpusPath");

    ArrayList<String> inputPaths = new ArrayList();
    List<Value> vs = p.list("inputPaths");
    for (Value v : vs) {
      inputPaths.add(v.toString());
    }

    if (p.containsKey("field")) {
	for(String f : p.get("field").split(",")){
	    //System.err.println("field list : " + f);
	    p.add("tokenizer/field", f);
	}
    }
    
    // ensure the index folder exists
    Utility.makeParentDirectories(indexPath);

    if (makeCorpus) {
      this.corpusParameters = new Parameters();
      this.corpusParameters.add("parallel", "true");
      this.corpusParameters.add("compressed", p.get("compressed", "true"));
      this.corpusParameters.add("filename", new File(p.get("corpusPath")).getAbsolutePath());
      this.corpusParameters.add("readerClass", CorpusReader.class.getName());
      this.corpusParameters.add("writerClass", CorpusWriter.class.getName());
    }

//    ArrayList<String> fieldNames = new ArrayList();
//    List<Value> vs2 = p.list("fieldNames");
//    for (Value v : vs2) {
//      job.add(getWriteExtentPostingsStage("writeExtentPostings", "numberedPostings", v.toString()));
//    }


    job.add(BuildStageTemplates.getSplitStage(inputPaths, DocumentSource.class));
    job.add(getParsePostingsStage());
    job.add(getWritePostingsStage("writePostings", "numberedPostings", "postings"));
    job.add(getWriteExtentPostingsStage("writeExtentPostings", "numberedExtentPostings", "field."));
    job.add(BuildStageTemplates.getWriteExtentsStage("writeExtents", new File(indexPath, "extents"), "numberedExtents"));
    job.add(BuildStageTemplates.getWriteNamesStage("writeNames", new File(indexPath, "names"), "numberedDocumentData"));
    job.add(BuildStageTemplates.getWriteLengthsStage("writeLengths", new File(indexPath, "lengths"), "numberedDocumentData"));
    job.add(BuildStageTemplates.getCollectionLengthStage("collectionLength", "numberedDocumentData", "collectionLength"));

    job.connect("inputSplit", "parsePostings", ConnectionAssignmentType.Each);
    job.connect("parsePostings", "writeLengths", ConnectionAssignmentType.Combined);
    job.connect("parsePostings", "writeNames", ConnectionAssignmentType.Combined);
    job.connect("parsePostings", "writeExtents", ConnectionAssignmentType.Combined);
    job.connect("parsePostings", "writePostings", ConnectionAssignmentType.Combined);
    job.connect("parsePostings", "writeExtentPostings", ConnectionAssignmentType.Combined);
    job.connect("parsePostings", "collectionLength", ConnectionAssignmentType.Combined);
    job.connect("collectionLength", "writePostings", ConnectionAssignmentType.Combined);

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
      job.connect("collectionLength", "writeStemmedPostings", ConnectionAssignmentType.Combined);
    }

    if (makeCorpus) {
      job.add(getParallelIndexKeyWriterStage("corpusIndexWriter", "corpusKeyData", this.corpusParameters));
      job.connect("parsePostings", "corpusIndexWriter", ConnectionAssignmentType.Combined);
    }

    return job;
  }
}
