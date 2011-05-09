// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.ngram;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.galagosearch.core.types.NgramFeature;
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
@InputClass(className = "org.galagosearch.core.ngram.Ngram")
@OutputClass(className = "org.galagosearch.core.types.NgramFeature")
public class NgramFeaturer extends StandardStep<Ngram, NgramFeature>{
  int bytes;
  MessageDigest hashFunction;
  
  public NgramFeaturer(TupleFlowParameters parameters) throws IOException, NoSuchAlgorithmException{
    // currently I'm setting the size of each feature to 8 bytes.
    // this means that the total ngram vocab shouldn't exceed ~= 2^60
    bytes = 8;
    hashFunction = MessageDigest.getInstance("MD5");
  }

  public void process(Ngram ngram) throws IOException {
    hashFunction.update(ngram.ngram);
    byte[] hashValue = hashFunction.digest();
    byte[] hashOutput = new byte[bytes];
    
    System.arraycopy(hashValue, (hashValue.length - bytes - 1), hashOutput, 0, hashOutput.length);
    
    // for now use the whole hashValue == 128 bits
    // later I will reduce it's size -> 64 bits
    processor.process(new NgramFeature(ngram.file, ngram.filePosition, hashValue));
  }
}
