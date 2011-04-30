// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.window;

import java.io.IOException;

import org.galagosearch.core.types.TextFeature;
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
@InputClass(className = "org.galagosearch.core.types.TextFeature", order={"+file", "+filePosition"})
@OutputClass(className = "org.galagosearch.core.types.TextFeature", order={"+file", "+filePosition"})
public class ExtractLocations extends StandardStep<TextFeature, TextFeature> {

  public void process(TextFeature tf) throws IOException {
    tf.feature = new byte[0];
    processor.process(tf);
  }
}
