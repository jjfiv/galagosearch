/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.tools;

import java.io.IOException;
import org.galagosearch.core.index.AbstractModifier;
import org.galagosearch.core.index.AdjacencyNameWriter;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.index.AdjacencyListWriter;
import org.galagosearch.core.parse.DistanceCalculator;
import org.galagosearch.core.parse.VocabularySource;
import org.galagosearch.core.types.Adjacency;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ConnectionAssignmentType;
import org.galagosearch.tupleflow.execution.InputStep;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.execution.OutputStep;
import org.galagosearch.tupleflow.execution.Stage;
import org.galagosearch.tupleflow.execution.Step;

/**
 *
 * @author irmarc
 */
public class BuildEditDistances {
    protected String indexPath;
    protected String partName;
    protected String method;
    protected int distance;

    public Stage getReadIndexStage() throws IOException {
        Stage stage = new Stage("readIndex");
        stage.addOutput("terms", new KeyValuePair.KeyOrder());

        Parameters p = new Parameters();
        p.set("filename", StructuredIndex.getPartPath(this.indexPath, this.partName));
        stage.add(new Step(VocabularySource.class, p));
        stage.add(new OutputStep("terms"));
        return stage;
    }

    public Stage getGenerateDistancesStage() {
        Stage stage = new Stage("generateEditDistances");
        stage.addInput("terms", new KeyValuePair.KeyOrder());
        stage.addOutput("edits", new Adjacency.SourceDestinationOrder());

        Parameters p = new Parameters();
        p.set("directory", this.indexPath);
        p.set("part", this.partName);
        p.set("distance", Integer.toString(this.distance));
        p.set("method", this.method);
        stage.add(new InputStep("terms"));
        stage.add(new Step(DistanceCalculator.class, p));
        stage.add(Utility.getSorter(new Adjacency.SourceDestinationOrder()));
        stage.add(new OutputStep("edits"));
        return stage;
    }

    public Stage getWriteDistancesStage() {
        Stage stage = new Stage("writeDistances");
        stage.addInput("edits", new Adjacency.SourceDestinationOrder());
        Parameters p = new Parameters();
        p.set("directory", this.indexPath);
        p.set("part", this.partName);
        p.set("name", "edits");
    	p.set("filename", AbstractModifier.getModifierName(this.indexPath, this.partName, "edits"));
        stage.add(new InputStep("edits"));
        stage.add(new Step(AdjacencyListWriter.class, p));
        return stage;
    }

    public Stage getWriteTermMappingStage() {
        Stage stage = new Stage("writeTermMapping");
        stage.addInput("terms", new KeyValuePair.KeyOrder());
        Parameters p = new Parameters();
        p.set("filename", AbstractModifier.getModifierName(this.indexPath, this.partName, "edits"));
        stage.add(new InputStep("terms"));
        stage.add(new Step(AdjacencyNameWriter.class, p));
        return stage;
    }

    public Job getIndexJob(Parameters p) throws IOException {
        Job job = new Job();
        this.indexPath = p.get("index");
        this.partName = p.get("part");
        this.distance = (int) p.get("distance", Integer.MAX_VALUE);
	this.method = p.get("method", "levenshtein");

        System.out.printf("Creating edit distances for part %s. Maximum allowable distance: %d\n",
                this.partName, this.distance);

        job.add(getReadIndexStage());
        job.add(getGenerateDistancesStage());
        job.add(getWriteDistancesStage());
        //job.add(getWriteTermMappingStage());
        
        job.connect("readIndex", "generateEditDistances", ConnectionAssignmentType.Each);
	//job.connect("readIndex", "writeTermMapping", ConnectionAssignmentType.Combined);
        job.connect("generateEditDistances", "writeDistances", ConnectionAssignmentType.Combined);


        return job;
    }
}
