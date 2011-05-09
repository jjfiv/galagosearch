/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.index;
import java.io.File;
import java.io.IOException;
import org.galagosearch.core.types.FieldNumberWordPosition;
import org.galagosearch.core.types.NumberWordPosition;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 *
 * @author jykim
 */
@InputClass(className = "org.galagosearch.core.types.FieldNumberWordPosition", order = {"+field", "+word", "+document", "+position"})
public class PositionFieldIndexWriter implements Processor<FieldNumberWordPosition> {

  NumberWordPosition.WordDocumentPositionOrder.TupleShredder shredder;
  PositionIndexWriter writer;
  String filePrefix;
  String prevField;
  private final TupleFlowParameters p;

  public PositionFieldIndexWriter(TupleFlowParameters p) {
    filePrefix = p.getXML().get("filename");
    this.p = p;
  }

  private void checkWriter(String fieldName) throws IOException
  {
    if( prevField == null  || prevField != fieldName ){
      if( shredder != null){
        shredder.close();
      }
      p.getXML().set("filename", filePrefix + fieldName);
      writer = new PositionIndexWriter(p);
      shredder = new NumberWordPosition.WordDocumentPositionOrder.TupleShredder(writer);
      prevField = fieldName;
    }
  }

  public void process(FieldNumberWordPosition object) throws IOException {
    checkWriter( object.field );
    shredder.process(new NumberWordPosition(object.document, object.word, object.position));
  }


  public void close() throws IOException {

      if( shredder != null){
        shredder.close();
      }
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getXML().containsKey("filename")) {
      handler.addError("PositionFieldIndexWriter requires a 'filename' parameter.");
      return;
    }

    String index = parameters.getXML().get("filename");
    Verification.requireWriteableFile(index, handler);
  }
}
