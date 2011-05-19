/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.parse;

import java.io.File;
import java.io.IOException;
import org.galagosearch.core.index.DocumentNameReader;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.types.DocumentIndicator;
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
@OutputClass(className = "org.galagosearch.core.types.DocumentIndicator")
public class IndicatorExtractor extends StandardStep<String, DocumentIndicator> {

  protected static class DocnameIndicator {
    String name;
    boolean ind;
  }

  private final DocumentNameReader.KeyIterator namesIterator;

  public IndicatorExtractor(TupleFlowParameters parameters) throws IOException {
    String namesPath = parameters.getXML().get("indexPath") + File.separator + "names.reverse";
    namesIterator = ((DocumentNameReader) StructuredIndex.openIndexPart(namesPath)).getIterator();
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
  protected DocnameIndicator convert(String line) {
    String[] split = line.split("\\s+");
    DocnameIndicator dni = new DocnameIndicator();
    dni.name = split[0];
    if(split.length == 2){
      dni.ind = Boolean.parseBoolean( split[1] );
    } else if(split.length == 1){
      dni.ind = true;
    }
    return dni;
  }


  @Override
  public void process(String line) throws IOException {
    // extract String bool pair
    DocnameIndicator dni = convert(line);

    // convert String to int
    DocumentIndicator di = new DocumentIndicator(getInternalDocId(dni.name), dni.ind);

    if(di.document > -1){
      processor.process(di);
    } // else ignore it
  }
}
