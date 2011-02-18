 // BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.ngram;

import java.io.IOException;

import org.galagosearch.core.parse.NumberedDocument;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * <p> Produces ngrams consisting of n words.</p>
 * 
 * Certain data features are maintained for the purpose
 * of filtering (Space Efficient Indexing Method)
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.parse.NumberedDocument")
@OutputClass(className = "org.galagosearch.core.ngram.Ngram")
public class NgramProducer extends StandardStep<NumberedDocument, Ngram> {
  int n;
  int currentFileId;
  long currentFilePosition;
  
  public NgramProducer(TupleFlowParameters parameters) throws IOException{
    this.n = Integer.parseInt(parameters.getXML().get("n"));
    currentFileId = -1;
    currentFilePosition = -1;
  }

  /*
   * n-gram format:
   *   "w1~w2~w3~"
   * 
   */
  public void process(NumberedDocument doc) throws IOException {
    
    //if this document is not big enough to contain any ngrams
    if(doc.terms.size() < n)
      return;

    if(currentFileId != doc.fileId){
      currentFileId = doc.fileId;
      currentFilePosition = 0;
    }
    StringBuilder sb;
    for(int position=0 ; position < ( doc.terms.size() - n + 1) ; position++ ){
      sb = new StringBuilder();
      for(int j=0 ; j < n; j++){
        sb.append( doc.terms.get((position+j)));
        sb.append( "~" );
      }
      byte[] ngram = Utility.fromString(sb.toString());
      processor.process(new Ngram(doc.fileId, currentFilePosition, doc.number, position, ngram ));
      currentFilePosition += 1;
    }
  }
}
