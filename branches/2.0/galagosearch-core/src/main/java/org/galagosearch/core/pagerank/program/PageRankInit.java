// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.pagerank.program;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.index.ExtractIndexDocumentNumbers;
import org.galagosearch.core.pagerank.init.DataFormatter;
import org.galagosearch.core.pagerank.init.FinishNumbering;
import org.galagosearch.core.pagerank.init.InitPREntry;
import org.galagosearch.core.pagerank.init.LinkFilter;
import org.galagosearch.core.pagerank.init.LinkFormatter;
import org.galagosearch.core.pagerank.init.PREntryWriter;
import org.galagosearch.core.pagerank.init.WriteLinks;
import org.galagosearch.core.types.HalfNumberedLink;
import org.galagosearch.core.parse.DocumentSource;
import org.galagosearch.core.parse.LinkExtractor;
import org.galagosearch.core.parse.NumberedDocumentDataExtractor;
import org.galagosearch.core.parse.TagTokenizer;
import org.galagosearch.core.parse.UniversalParser;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.core.types.ExtractedLink;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.core.types.NumberedLink;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ConnectionAssignmentType;
import org.galagosearch.tupleflow.execution.ConnectionPointType;
import org.galagosearch.tupleflow.execution.InputStep;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.execution.MultiStep;
import org.galagosearch.tupleflow.execution.OutputStep;
import org.galagosearch.tupleflow.execution.Stage;
import org.galagosearch.tupleflow.execution.StageConnectionPoint;
import org.galagosearch.tupleflow.execution.Step;

/**
 * Adapted from Stanford's pagerank code (Summer 2010)
 *
 * Job Plan:
 *  1: extracts links
 *  2: numbers links
 *  3: initializes pagerank values
 *  4: writes links + initial values to files
 *
 * @author sjh, schiu
 */
public class PageRankInit {

  private String indexPath;
  private String pagerankFolder;

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

  public Stage getParseCollectionStage() {
    // reads through the corpus -- extracts all required data

    Stage stage = new Stage("parseCollection");

    stage.add(new StageConnectionPoint(
            ConnectionPointType.Input,
            "splits", new DocumentSplit.FileIdOrder()));

    stage.add(new StageConnectionPoint(
            ConnectionPointType.Output,
            "links", new ExtractedLink.DestUrlOrder()));
    stage.add(new StageConnectionPoint(
            ConnectionPointType.Output,
            "numberedDocumentData", new NumberedDocumentData.NumberOrder()));

    stage.add(new InputStep("splits"));
    stage.add(new Step(UniversalParser.class));
    stage.add(new Step(TagTokenizer.class));

    Parameters p = new Parameters();
    p.add("indexPath", indexPath);
    stage.add(new Step(ExtractIndexDocumentNumbers.class, p));

    // now we have tag-tokenized NumberedDocuments
    // now we'll want to extract 'some stuff' and output various streams

    MultiStep multi = new MultiStep();

    ArrayList<Step> links = new ArrayList<Step>();
    Parameters p2 = new Parameters();
    p2.add("class", "org.galagosearch.core.parse.NumberedDocument");
    links.add(new Step(LinkExtractor.class, p2));
    links.add(new Step(LinkFormatter.class));
    links.add(Utility.getSorter(new ExtractedLink.DestUrlOrder()));
    links.add(new OutputStep("links"));

    ArrayList<Step> data = new ArrayList<Step>();
    data.add(new Step(NumberedDocumentDataExtractor.class));
    data.add(new Step(DataFormatter.class)); // fixes the urls as required
    data.add(Utility.getSorter(new NumberedDocumentData.NumberOrder()));
    data.add(new OutputStep("numberedDocumentData"));

    multi.groups.add(links);
    multi.groups.add(data);
    stage.add(multi);

    return stage;
  }

  public Stage getDocumentSortByUrlStage() {

    Stage stage = new Stage("sortDocumentsByUrl");

    stage.add(new StageConnectionPoint(
            ConnectionPointType.Input,
            "numberedDocumentData", new NumberedDocumentData.NumberOrder()));
    stage.add(new StageConnectionPoint(
            ConnectionPointType.Output,
            "numberedDocumentDataUrlSort", new NumberedDocumentData.UrlOrder()));

    stage.add(new InputStep("numberedDocumentData"));
    stage.add(Utility.getSorter(new NumberedDocumentData.UrlOrder()));
    stage.add(new OutputStep("numberedDocumentDataUrlSort"));

    return stage;
  }

  public static Stage getLinkFilterStage() {
    Stage stage = new Stage("LinkFilter");

    String srcInputName = "numberedDocumentDataUrlSort";
    String destInputName = "links";

    String outputName = "halfNumLinks";

    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            srcInputName, new NumberedDocumentData.UrlOrder()));
    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            destInputName, new ExtractedLink.DestUrlOrder()));
    stage.add(new StageConnectionPoint(ConnectionPointType.Output,
            outputName, new HalfNumberedLink.SrcOrder()));


    Parameters params = new Parameters();
    params.add("srcLinks", srcInputName);
    params.add("destLinks", destInputName);

    stage.add(new Step(LinkFilter.class, params));
    stage.add(Utility.getSorter(new HalfNumberedLink.SrcOrder()));
    stage.add(new OutputStep(outputName));

    return stage;
  }

  public static Stage getFinishLinkNumberingStage() {
    Stage stage = new Stage("FinishLinkNum");

    String numberedDataName = "numberedDocumentDataUrlSort";
    String linksName = "halfNumLinks";
    String numLinksName = "numLinks";

    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            numberedDataName, new NumberedDocumentData.UrlOrder()));
    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            linksName, new HalfNumberedLink.SrcOrder()));

    stage.add(new StageConnectionPoint(ConnectionPointType.Output,
            numLinksName, new NumberedLink.SourceOrder()));

    Parameters params = new Parameters();
    params.add("numberedData", numberedDataName);
    params.add("sortedLinks", linksName);

    stage.add(new Step(FinishNumbering.class, params));
    
    // now we need to ensure they are in numerical order
    stage.add(Utility.getSorter(new NumberedLink.SourceOrder()));
    stage.add(new OutputStep(numLinksName));

    return stage;
  }

  // writes a set of docId1-docId2 pairs
  // (indicating that docId1 links to docId2)
  public Stage getWriteLinksStage() {
    Stage stage = new Stage("WriteLinks");

    String numLinksName = "numLinks";

    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            numLinksName, new NumberedLink.SourceOrder()));

    Parameters params = new Parameters();
    params.add("links", this.pagerankFolder + File.separator + "links.pr");

    stage.add(new InputStep(numLinksName));
    stage.add(new Step(WriteLinks.class, params));

    return stage;
  }

  public Stage getWritePREntriesStage() {
    Stage stage = new Stage("WritePREntries");

    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            "numberedDocumentData", new NumberedDocumentData.NumberOrder()));

    Parameters params = new Parameters();
    params.add("index", this.indexPath);
    params.add("entries", this.pagerankFolder + File.separator + "entries.pr");
    params.add("manifest", this.pagerankFolder + File.separator + "manifest.pr");

    stage.add(new InputStep("numberedDocumentData"));
    stage.add(new Step(InitPREntry.class, params));
    stage.add(new Step(PREntryWriter.class, params));

    return stage;
  }

  public Job makeJob(Parameters p) throws IOException {

    Job job = new Job();
    this.indexPath = new File(p.get("indexPath")).getAbsolutePath(); // fail if no path.
    this.pagerankFolder = new File(p.get("pagerankTemp")).getAbsolutePath(); // fail if no path.
    ArrayList<String> inputPaths = new ArrayList();
    List<Value> vs = p.list("inputPaths");
    for (Value v : vs) {
      inputPaths.add(v.toString());
    }

    // ensure the index folder is an index
    StructuredIndex i = new StructuredIndex(indexPath);
    i.close();

    job.add(getSplitStage(inputPaths));
    job.add(getParseCollectionStage());
    job.add(getDocumentSortByUrlStage());
    job.add(getLinkFilterStage());
    job.add(getFinishLinkNumberingStage());
    job.add(getWriteLinksStage());
    job.add(getWritePREntriesStage());

    job.connect("inputSplit", "parseCollection", ConnectionAssignmentType.Each);
    job.connect("parseCollection", "sortDocumentsByUrl", ConnectionAssignmentType.Each);
    job.connect("collectionLength", "WritePREntries", ConnectionAssignmentType.Combined);
    job.connect("parseCollection", "WritePREntries", ConnectionAssignmentType.Combined);
    job.connect("parseCollection", "LinkFilter", ConnectionAssignmentType.Each);
    job.connect("sortDocumentsByUrl", "LinkFilter", ConnectionAssignmentType.Each);
    job.connect("LinkFilter", "FinishLinkNum", ConnectionAssignmentType.Each);
    job.connect("sortDocumentsByUrl", "FinishLinkNum", ConnectionAssignmentType.Each);
    job.connect("FinishLinkNum", "WriteLinks", ConnectionAssignmentType.Combined);


    return job;
  }
}
