// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.ngram;

import java.io.IOException;

import org.galagosearch.core.types.NgramFeature;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * <p> Discards word data - Only locations are maintained.
 * This leads to some space savings in any output files.
 * </p>
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.NgramFeature", order={"+file", "+filePosition"})
@OutputClass(className = "org.galagosearch.core.types.NgramFeature", order={"+file", "+filePosition"})
public class ExtractLocations extends StandardStep<NgramFeature, NgramFeature> {

  public void process(NgramFeature ngram) throws IOException {
    processor.process(new NgramFeature(ngram.file, ngram.filePosition, new byte[0]));
  }

}
