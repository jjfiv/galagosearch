// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.galagosearch.core.index.ExtentIndexWriter;
import org.galagosearch.core.index.PositionIndexWriter;
import org.galagosearch.core.parse.DocumentSource;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.core.types.NumberWordPosition;
import org.galagosearch.core.types.NumberedExtent;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.execution.ConnectionAssignmentType;
import org.galagosearch.tupleflow.execution.ConnectionPointType;
import org.galagosearch.tupleflow.execution.InputStep;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.execution.OutputStep;
import org.galagosearch.tupleflow.execution.Stage;
import org.galagosearch.tupleflow.execution.StageConnectionPoint;
import org.galagosearch.tupleflow.execution.Step;

/**
 *
 * Builds an index using a faster method 
 * (it requires one less sort of the posting lists)
 *
 * @author sjh
 */
public class BuildParallelIndex extends BuildFastIndex {

    protected Parameters extentParameters;
    protected Parameters postingsParameters;
    protected Parameters stemmedParameters;
    protected int indexShards;


    public Stage getWritePostingsStage(String stageName, String inputName, Parameters p) {
        Stage stage = new Stage(stageName);

        stage.add(new StageConnectionPoint(
                ConnectionPointType.Input, inputName,
                new NumberWordPosition.WordDocumentPositionOrder()));
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Output, inputName + "Keys",
                new KeyValuePair.KeyOrder()));

        stage.add(new InputStep(inputName));
        stage.add(new Step(PositionIndexWriter.class, p));
        stage.add(new OutputStep(inputName + "Keys"));

        return stage;
    }

    public Stage getWriteExtentsStage(String stageName, String inputName, Parameters p) {
        Stage stage = new Stage(stageName);

        stage.add(new StageConnectionPoint(
                ConnectionPointType.Input, inputName,
                new NumberedExtent.ExtentNameNumberBeginOrder()));
        stage.add(new StageConnectionPoint(
                ConnectionPointType.Output, inputName + "Keys",
                new KeyValuePair.KeyOrder()));

        stage.add(new InputStep(inputName));
        stage.add(new Step(ExtentIndexWriter.class, p));
        stage.add(new OutputStep(inputName + "Keys"));

        return stage;
    }
 
    public Job getIndexJob(Parameters p) throws IOException {

        Job job = new Job();
        this.buildParameters = p;
        this.stemming = p.get("stemming", true);
        this.useLinks = p.get("links", false);
        this.indexPath = new File(p.get("indexPath")); // fail if no path.
        this.makeCorpus = p.containsKey("corpusPath");
        this.indexShards = (int) p.get("indexShards", 11);

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

        this.postingsParameters = new Parameters();
        this.postingsParameters.add("parallel", "true");
        this.postingsParameters.add("hashMod", Integer.toString(indexShards));
        this.postingsParameters.add("filename", new File(indexPath, "postings").getCanonicalPath());

        this.extentParameters = new Parameters();
        this.extentParameters.add("parallel", "true");
        this.extentParameters.add("hashMod", Integer.toString(indexShards));
        this.extentParameters.add("filename", new File(indexPath, "extents").getCanonicalPath());

        job.add(BuildStageTemplates.getSplitStage(inputPaths, DocumentSource.class));
        job.add(getParsePostingsStage());
        job.add(getWritePostingsStage("writePostings", "numberedPostings", postingsParameters));
        job.add(getWriteExtentsStage("writeExtents", "numberedExtents", extentParameters));
        job.add(BuildStageTemplates.getWriteManifestStage("writeManifest", new File(indexPath, "manifest"), "collectionLength", "postings"));
        job.add(BuildStageTemplates.getWriteNamesStage("writeNames", new File(indexPath, "names"), "numberedDocumentData"));
        job.add(BuildStageTemplates.getWriteLengthsStage("writeLengths", new File(indexPath, "lengths"), "numberedDocumentData"));
        job.add(BuildStageTemplates.getCollectionLengthStage("collectionLength", "numberedDocumentData", "collectionLength"));


        // parallel writers
        job.add(getParallelIndexKeyWriterStage("writePostingKeys", "numberedPostingsKeys", postingsParameters));
        job.add(getParallelIndexKeyWriterStage("writeExtentKeys", "numberedExtentsKeys", extentParameters));

        job.connect("inputSplit", "parsePostings", ConnectionAssignmentType.Each);
        job.connect("parsePostings", "writeLengths", ConnectionAssignmentType.Combined);
        job.connect("parsePostings", "writeNames", ConnectionAssignmentType.Combined);
        job.connect("parsePostings", "collectionLength", ConnectionAssignmentType.Combined);
        job.connect("parsePostings", "writeExtents", ConnectionAssignmentType.Each, new String[]{"+extentName"}, indexShards);
        job.connect("parsePostings", "writePostings", ConnectionAssignmentType.Each, new String[]{"+word"}, indexShards);
        job.connect("collectionLength", "writeManifest", ConnectionAssignmentType.Combined);

        // parallel writers
        job.connect("writePostings", "writePostingKeys", ConnectionAssignmentType.Combined);
        job.connect("writeExtents", "writeExtentKeys", ConnectionAssignmentType.Combined);

        if (useLinks) {
            job.add(getParseLinksStage());
            job.add(getLinkCombineStage());

            job.connect("inputSplit", "parseLinks", ConnectionAssignmentType.Each);
            job.connect("parseLinks", "linkCombine", ConnectionAssignmentType.Combined); // this should be Each, but the subsequent connection wouldnt work
            job.connect("linkCombine", "parsePostings", ConnectionAssignmentType.Combined);
        }

        if (stemming) {
            this.stemmedParameters = new Parameters();
            this.stemmedParameters.add("parallel", "true");
            this.stemmedParameters.add("hashMod", Integer.toString(indexShards));
            this.stemmedParameters.add("filename", indexPath + File.separator + "stemmedPostings");

            job.add(getWritePostingsStage("writeStemmedPostings",
                    "numberedStemmedPostings",
                    stemmedParameters));
            job.add(getParallelIndexKeyWriterStage("writeStemmedPostingKeys", "numberedStemmedPostingsKeys", stemmedParameters));

            job.connect("parsePostings", "writeStemmedPostings", ConnectionAssignmentType.Each, new String[]{"+word"}, indexShards);
            job.connect("writeStemmedPostings", "writeStemmedPostingKeys", ConnectionAssignmentType.Combined);

        }

        if (makeCorpus) {
            job.add(getParallelIndexKeyWriterStage("corpusIndexWriter", "corpusKeyData", this.corpusParameters));
            job.connect("parsePostings", "corpusIndexWriter", ConnectionAssignmentType.Combined);
        }

        return job;
    }
}
