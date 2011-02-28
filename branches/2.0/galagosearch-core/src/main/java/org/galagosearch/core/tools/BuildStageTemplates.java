/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import org.galagosearch.core.index.DocumentLengthsWriter;
import org.galagosearch.core.index.DocumentNameWriter;
import org.galagosearch.core.index.ExtentIndexWriter;
import org.galagosearch.core.index.ExtentValueIndexWriter;
import org.galagosearch.core.index.ManifestWriter;
import org.galagosearch.core.parse.CollectionLengthCounterNDD;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.core.types.NumberedExtent;
import org.galagosearch.core.types.NumberedValuedExtent;
import org.galagosearch.tupleflow.ExNihiloSource;
import org.galagosearch.tupleflow.Order;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ConnectionPointType;
import org.galagosearch.tupleflow.execution.InputStep;
import org.galagosearch.tupleflow.execution.OutputStep;
import org.galagosearch.tupleflow.execution.Stage;
import org.galagosearch.tupleflow.execution.StageConnectionPoint;
import org.galagosearch.tupleflow.execution.Step;
import org.galagosearch.tupleflow.types.XMLFragment;

/**
 *
 * @author irmarc
 */
public class BuildStageTemplates {

  // Cannot instantiate - just a container class
  private BuildStageTemplates() {
  }

  /**
   * Writes document lengths to a document lengths file.
   */
  public static Stage getWriteLengthsStage(File destination) throws IOException {
    Stage stage = new Stage("writeLengths");

    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            "numberedDocumentData", new NumberedDocumentData.NumberOrder()));
    Parameters p = new Parameters();
    p.add("filename", new File(destination, "lengths").getCanonicalPath());
    stage.add(new InputStep("numberedDocumentData"));
    stage.add(new Step(DocumentLengthsWriter.class, p));

    return stage;
  }

  /**
   * Write out document count and collection length information.
   */
  public static Stage getWriteManifestStage(File destination) throws IOException {
    Stage stage = new Stage("writeManifest");

    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            "collectionLength",
            new XMLFragment.NodePathOrder()));
    stage.add(new InputStep("collectionLength"));
    Parameters p = new Parameters();
    p.add("filename", new File(destination, "manifest").getAbsolutePath());
    stage.add(new Step(ManifestWriter.class, p));
    return stage;
  }

  public static Stage getWriteDatesStage(File destination) throws IOException {
    Stage stage = new Stage("writeDates");

    stage.add(new StageConnectionPoint(
            ConnectionPointType.Input, "numberedDateExtents",
            new NumberedValuedExtent.ExtentNameNumberBeginOrder()));
    Parameters p = new Parameters();
    p.add("filename", new File(destination, "dates").getCanonicalPath());
    stage.add(new Step(ExtentValueIndexWriter.class));

    return stage;
  }

  public static Stage getCollectionLengthStage() {

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

  /**
   * Writes document names to a document names file.
   */
  public static Stage getWriteNamesStage(File destination) throws IOException {
    Stage stage = new Stage("writeNames");

    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            "numberedDocumentData", new NumberedDocumentData.NumberOrder()));
    Parameters p = new Parameters();
    p.add("filename", new File(destination, "names").getCanonicalPath());
    stage.add(new InputStep("numberedDocumentData"));
    stage.add(new Step(DocumentNameWriter.class, p));
    return stage;
  }

  public static Stage getWriteExtentsStage(File destination) throws IOException {
    Stage stage = new Stage("writeExtents");

    stage.add(new StageConnectionPoint(
            ConnectionPointType.Input, "numberedExtents",
            new NumberedExtent.ExtentNameNumberBeginOrder()));

    stage.add(new InputStep("numberedExtents"));
    Parameters p = new Parameters();
    p.add("filename", new File(destination, "extents").getCanonicalPath());
    stage.add(new Step(ExtentIndexWriter.class, p));
    return stage;
  }

  public static Stage getSplitStage(ArrayList<String> inputPaths, Class<? extends ExNihiloSource<DocumentSplit>> sourceClass) throws IOException {
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

    stage.add(new Step(sourceClass, p));
    stage.add(Utility.getSorter(new DocumentSplit.FileIdOrder()));
    stage.add(new OutputStep("splits"));
    return stage;
  }

  public static ArrayList<Step> getExtractionSteps(String outputName, Class extractionClass, Order sortOrder) {
    ArrayList<Step> steps = new ArrayList<Step>();
    steps.add(new Step(extractionClass));
    steps.add(Utility.getSorter(sortOrder));
    steps.add(new OutputStep(outputName));
    return steps;
  }
}
