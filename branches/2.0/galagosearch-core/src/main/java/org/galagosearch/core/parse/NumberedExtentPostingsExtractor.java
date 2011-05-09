/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.parse;

import java.io.IOException;
import org.galagosearch.core.types.FieldNumberWordPosition;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author jykim
 */

@InputClass(className = "org.galagosearch.core.parse.NumberedDocument")
@OutputClass(className = "org.galagosearch.core.types.FieldNumberWordPosition")
@Verified

public class NumberedExtentPostingsExtractor extends StandardStep<NumberedDocument, FieldNumberWordPosition> {

  @Override
  public void process(NumberedDocument object) throws IOException {
    
    int number = object.number;
    for (Tag tag : object.tags) {
      String field = tag.name;
      for (int position = tag.begin; position < tag.end ; position++) {
        byte[] word = Utility.fromString( object.terms.get( position ));
        processor.process(new FieldNumberWordPosition( field, number, word, position));
      }
    }
  }
}
