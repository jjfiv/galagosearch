package org.galagosearch.core.mergeindex;

import java.io.IOException;
import java.util.ArrayList;

import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.execution.Verified;
import org.galagosearch.tupleflow.types.XMLFragment;

@Verified
@InputClass( className = "org.galagosearch.tupleflow.Parameters")
@OutputClass (className = "org.galagosearch.tupleflow.types.XMLFragment")
public class ManifestMerger extends StandardStep<Parameters, XMLFragment> {
  ArrayList<Parameters> manifests = new ArrayList();
  
  public void process(Parameters p) throws IOException {
    manifests.add(p);
  }

  public void close() throws IOException {
    long collectionLength = 0L;
    long documentCount = 0L;
    int documentNumberOffset = Integer.MAX_VALUE;
    
    for(Parameters p : manifests){
      collectionLength += (long) p.get("collectionLength", 0);
      documentCount += (long) p.get("documentCount", 0);
      documentNumberOffset = Math.min(documentNumberOffset, (int) p.get("documentNumberOffset", 0));
    }
    
    processor.process(new XMLFragment("collectionLength", Long.toString(collectionLength)));
    processor.process(new XMLFragment("documentCount", Long.toString(documentCount)));

    if(documentNumberOffset > 0){
        processor.process(new XMLFragment("documentNumberOffset", Long.toString(documentNumberOffset)));
    }

    processor.close();
  }
}
