// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.ngram;

import java.io.IOException;

import org.galagosearch.core.types.NumberWordPosition;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * 
 * Converts Ngram objects to NumberWordPosition objects
 * Some data is discarded
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.ngram.Ngram")
@OutputClass(className = "org.galagosearch.core.types.NumberWordPosition")
public class NgramToNumberWordPosition extends StandardStep<Ngram, NumberWordPosition> {

  public void process(Ngram ngram) throws IOException {
    processor.process( new NumberWordPosition(ngram.document, ngram.ngram, ngram.position));
  }

}
