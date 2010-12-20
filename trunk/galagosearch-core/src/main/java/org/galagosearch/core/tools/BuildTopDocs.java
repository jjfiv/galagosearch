/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.tools;

import java.io.IOException;
import org.galagosearch.core.index.TopDocsWriter;
import org.galagosearch.core.parse.TopDocsScanner;
import org.galagosearch.core.parse.VocabularySource;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.core.types.TopDocsEntry;
import org.galagosearch.tupleflow.Parameters;
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
 * @author marc
 */
public class BuildTopDocs {
    protected String indexPath;
    protected String partName;
    protected String topdocs_size;
    protected String list_min_size;

    public Stage getReadIndexStage() throws IOException {
        Stage stage = new Stage("readIndex");
        stage.add(new StageConnectionPoint(ConnectionPointType.Output, "terms",
                new KeyValuePair.KeyOrder()));

        Parameters p = new Parameters();
        p.set("directory", this.indexPath);
        p.set("part", this.partName);
        stage.add(new Step(VocabularySource.class, p));
        stage.add(new OutputStep("terms"));
        return stage;
    }

    public Stage getIterateOverPostingListsStage() {
        Stage stage = new Stage("iterateOverPostingLists");
        stage.add(new StageConnectionPoint(ConnectionPointType.Input, "terms",
                new KeyValuePair.KeyOrder()));
        stage.add(new StageConnectionPoint(ConnectionPointType.Output, "topdocs",
                new TopDocsEntry.WordDocumentOrder()));

        Parameters p = new Parameters();
        p.set("directory", this.indexPath);
        p.set("part", this.partName);
        p.set("size", this.topdocs_size);
        p.set("minlength", this.list_min_size);
        stage.add(new InputStep("terms"));
        stage.add(new Step(TopDocsScanner.class, p));
        stage.add(new OutputStep("topdocs"));
        return stage;
    }

    public Stage getWriteTopDocsStage() {
        Stage stage = new Stage("writeTopDocs");
        stage.add(new StageConnectionPoint(ConnectionPointType.Input, "topdocs",
                new TopDocsEntry.WordDocumentOrder()));
        Parameters p = new Parameters();
        p.set("directory", this.indexPath);
        stage.add(new InputStep("topdocs"));
        stage.add(new Step(TopDocsWriter.class, p));
        return stage;
    }

    public Job getIndexJob(Parameters p) throws IOException {
        Job job = new Job();
        this.indexPath = p.get("index");
        this.partName = p.get("part");
        this.topdocs_size = p.get("size", Integer.toString(Integer.MAX_VALUE));
        this.list_min_size = p.get("minlength", Long.toString(Long.MAX_VALUE));

        job.add(getReadIndexStage());
        job.add(getIterateOverPostingListsStage());
        job.add(getWriteTopDocsStage());

        job.connect("readIndex", "iterateOverPostingLists", ConnectionAssignmentType.Each);
        job.connect("iterateOverPostingLists", "writeTopDocs", ConnectionAssignmentType.Combined);

        return job;
    }
}
