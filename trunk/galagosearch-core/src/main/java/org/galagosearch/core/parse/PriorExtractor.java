/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.parse;

import java.io.File;
import java.io.IOException;
import org.galagosearch.core.index.DocumentNameReader;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.types.DocumentIndicator;
import org.galagosearch.core.types.DocumentProbability;
import org.galagosearch.core.types.NumberWordPosition;
import org.galagosearch.core.types.NumberWordProbability;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * Vanilla implementation
 * 
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "java.lang.String")
@OutputClass(className = "org.galagosearch.core.types.NumberWordProbability")
public class PriorExtractor extends StandardStep<String, NumberWordProbability> {

  private final DocumentNameReader.KeyIterator namesIterator;
  private boolean applylog;

  public PriorExtractor(TupleFlowParameters parameters) throws IOException {
    String namesPath = parameters.getXML().get("indexPath") + File.separator + "names.reverse";
    namesIterator = ((DocumentNameReader) StructuredIndex.openIndexPart(namesPath)).getIterator();

    // type of scores being read in:
    String priorType = parameters.getXML().get("priorType", "r");
    if( priorType.startsWith("r")){
      applylog = false;
    } else if(priorType.startsWith("p")) {
      applylog = true;
    } else if(priorType.startsWith("l")) {
      applylog = false;
    }
  }

  private int getInternalDocId(String docName) throws IOException {
    if(namesIterator.moveToKey(docName)){
      return namesIterator.getCurrentIdentifier();
    } else {
      return -1;
    }
  }

  /* Overridable function
   * 
   *  - transforms a string into a doc name + boolean pair
   *  - default tries to parse:
   *   "docname \t [true|false]"
   *  - or
   *   "docname" // assuming true
   */
  protected DocumentProbability convert(String line) {
    String[] split = line.split("\\s+");
    if(split.length == 2){
      DocumentProbability dp = new DocumentProbability();
      dp.document = split[0];
      dp.probability = Double.parseDouble( split[1] );

      if(applylog){
        dp.probability = Math.log(dp.probability);
      }
      return dp;
    }
    return null;
  }


  @Override
  public void process(String line) throws IOException {
    // extract String bool pair
    DocumentProbability dp = convert(line);
    // convert String to int
    int internalDocId = getInternalDocId(dp.document);
    // check that the document number is valid
    if(internalDocId > -1){
      NumberWordProbability nwp = new NumberWordProbability(internalDocId, new byte[0], dp.probability);
      processor.process(nwp);
    } // else ignore document
  }
}
