// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.mergeindex.parallel;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;

import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.Verified;

@Verified
@InputClass( className = "org.galagosearch.core.mergeindex.parallel.MappedDocument" )
@OutputClass( className = "org.galagosearch.core.types.DocumentSplit", order = {"+fileId"})
public class NumberMappingWriter extends StandardStep<MappedDocument, DocumentSplit>{

  Counter mappedDocuments;
  File mappingFolder;
  
  HashMap<Integer, DataOutputStream> openFiles = new HashMap();
  HashMap<Integer, Integer> filePositions = new HashMap();
  
  public NumberMappingWriter(TupleFlowParameters parameters) throws IOException, IncompatibleProcessorException{
    mappingFolder = new File(parameters.getXML().get("filename"));
    mappingFolder.mkdir();
    
    mappedDocuments = parameters.getCounter("Mapped Documents");
  }
  
  public void process(MappedDocument mapdoc) throws IOException {
    // make two BulkTreeItems (forward, reverse)
    // insert each into the sorter/writers
    if(! openFiles.containsKey(mapdoc.indexId)){
      DataOutputStream out = new DataOutputStream(new FileOutputStream(mappingFolder + File.separator + mapdoc.indexId));
      openFiles.put(mapdoc.indexId, out);
      filePositions.put(mapdoc.indexId, 0);
    }

    DataOutputStream out = openFiles.get(mapdoc.indexId);
    int written = filePositions.get(mapdoc.indexId);

    assert written <= mapdoc.oldDocumentNumber;
    while(written < mapdoc.oldDocumentNumber){
      out.writeInt(-1); // neg 1 indicates no mapping
      written++;
    }
    out.writeInt(mapdoc.newDocumentNumber);
    written++;
    filePositions.put(mapdoc.indexId, written);


    if(mappedDocuments != null)
      mappedDocuments.increment();
  }

  public void close() throws IOException {
    for(int indexId : openFiles.keySet()){
      DataOutputStream out = openFiles.get(indexId);
      //out.writeInt( filePositions.get(indexId) );
      out.close();
    }
    processor.process( new DocumentSplit(mappingFolder.getAbsolutePath(),"",false,new byte[0],new byte[0],0,0));
    processor.close();
  }
}
