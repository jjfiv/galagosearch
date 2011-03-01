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
import org.galagosearch.tupleflow.Processor;
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

  public static Stage getGenericWriteStage(String stageName, File destination, String inputPipeName,
          Class<? extends org.galagosearch.tupleflow.Step> writer, Order dataOrder) throws IOException {
    Stage stage = new Stage(stageName);
    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            inputPipeName, dataOrder));
    Parameters p = new Parameters();
    p.add("filename", destination.getCanonicalPath());
    stage.add(new InputStep(inputPipeName));
    stage.add(new Step(writer, p));
    return stage;
  }

  public static ArrayList<Step> getExtractionSteps(String outputName, Class extractionClass, Order sortOrder) {
    ArrayList<Step> steps = new ArrayList<Step>();
    steps.add(new Step(extractionClass));
    steps.add(Utility.getSorter(sortOrder));
    steps.add(new OutputStep(outputName));
    return steps;
  }

  /**
   * Writes document lengths to a document lengths file.
   */
  public static Stage getWriteLengthsStage(String stageName, File destination, String inputPipeName) throws IOException {
    return getGenericWriteStage(stageName, destination, inputPipeName,
            DocumentLengthsWriter.class, new NumberedDocumentData.NumberOrder());
  }

  /**
   * Write out document count and collection length information.
   */
  public static Stage getWriteManifestStage(String stageName, File destination, String inputPipeName) throws IOException {
    return getGenericWriteStage(stageName, destination, inputPipeName,
            ManifestWriter.class, new XMLFragment.NodePathOrder());
  }

  public static Stage getWriteDatesStage(String stageName, File destination, String inputPipeName) throws IOException {
    return getGenericWriteStage(stageName, destination, inputPipeName,
            ExtentValueIndexWriter.class, new NumberedValuedExtent.ExtentNameNumberBeginOrder());
  }

  /**
   * Writes document names to a document names file.
   */
  public static Stage getWriteNamesStage(String stageName, File destination, String inputPipeName) throws IOException {
    return getGenericWriteStage(stageName, destination, inputPipeName,
            DocumentNameWriter.class, new NumberedDocumentData.NumberOrder());
  }

  public static Stage getWriteExtentsStage(String stageName, File destination, String inputPipeName) throws IOException {
    return getGenericWriteStage(stageName, destination, inputPipeName,
            ExtentIndexWriter.class, new NumberedExtent.ExtentNameNumberBeginOrder());
  }

  public static Stage getCollectionLengthStage(String stageName, String inputPipeName, String outputPipeName) {
    Stage stage = new Stage(stageName);

    stage.add(new StageConnectionPoint(
            ConnectionPointType.Input, inputPipeName,
            new NumberedDocumentData.NumberOrder()));
    stage.add(new StageConnectionPoint(
            ConnectionPointType.Output, outputPipeName,
            new XMLFragment.NodePathOrder()));

    stage.add(new InputStep(inputPipeName));
    stage.add(new Step(CollectionLengthCounterNDD.class));
    stage.add(Utility.getSorter(new XMLFragment.NodePathOrder()));
    stage.add(new OutputStep(outputPipeName));

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
}
