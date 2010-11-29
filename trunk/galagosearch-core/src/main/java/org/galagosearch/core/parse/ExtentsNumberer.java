// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.parse;

import java.io.IOException;
import java.util.HashMap;
import org.galagosearch.core.types.DocumentExtent;
import org.galagosearch.core.types.NumberWordPosition;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.core.types.NumberedExtent;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.TypeReader;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@InputClass(className = "org.galagosearch.core.types.DocumentExtent")
@OutputClass(className = "org.galagosearch.core.types.NumberedExtent")
@Verified
public class ExtentsNumberer extends StandardStep<DocumentExtent, NumberedExtent> {
  TypeReader<NumberedDocumentData> reader;
  NumberedDocumentData currentNDD;
  
    public void process(DocumentExtent object) throws IOException {
      assert Utility.compare(currentNDD.identifier, object.identifier) <= 0 : 
        "PositionPostingNumberer is getting postings in the wrong order somehow.";
      
      while((currentNDD != null) &&
          (Utility.compare(currentNDD.identifier, object.identifier) < 0 )){
        currentNDD = reader.read();
      }
      
      if((currentNDD != null) &&
        (Utility.compare(currentNDD.identifier, object.identifier) == 0)){
        processor.process(new NumberedExtent(Utility.fromString(object.extentName),
            currentNDD.number, object.begin, object.end));

      } else {
        throw new IOException("Ran out of Document Numbers or Found Unknown Document");
      }
    }

    public ExtentsNumberer(TupleFlowParameters parameters) throws IOException {
      reader = parameters.getTypeReader("numberedDocumentData");
      currentNDD = reader.read();
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        Verification.verifyTypeReader("nubmeredDocumentData", DocumentExtent.class, parameters,
                handler);
    }
}
