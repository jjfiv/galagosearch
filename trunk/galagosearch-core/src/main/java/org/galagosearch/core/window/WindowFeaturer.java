// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.window;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.galagosearch.core.types.TextFeature;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * Converts ngrams to NgramFeatures
 * This keeps track of the file and location 
 *  that the ngram was extracted from.
 * 
 * An MD5 hash value is used instead of the original ngram
 * This reduces the size of the output.
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.window.Window")
@OutputClass(className = "org.galagosearch.core.types.TextFeature")
public class WindowFeaturer extends StandardStep<Window, TextFeature> {

  int bytes;
  MessageDigest hashFunction;

  public WindowFeaturer(TupleFlowParameters parameters) throws IOException, NoSuchAlgorithmException {
    hashFunction = MessageDigest.getInstance("MD5");
  }

  public void process(Window w) throws IOException {
    hashFunction.update(w.data);
    byte[] hashValue = hashFunction.digest();
    processor.process(new TextFeature(w.file, w.filePosition, hashValue));
  }
}
