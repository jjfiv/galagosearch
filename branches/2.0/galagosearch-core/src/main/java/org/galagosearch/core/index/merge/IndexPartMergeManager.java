/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.index.merge;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.index.StructuredIndexPartReader;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.tupleflow.FakeParameters;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.TypeReader;
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass( className="org.galagosearch.core.types.DocumentSplit")
public class IndexPartMergeManager implements Processor<DocumentSplit> {

  TupleFlowParameters parameters;
  String part;

  HashMap<StructuredIndexPartReader, Integer> indexPartReaders = new HashMap();
  String mergerClassName = null;
  String writerClassName = null;
  DocumentMappingReader mappingData = null;

  public IndexPartMergeManager(TupleFlowParameters parameters) throws IOException {
    this.parameters = parameters;
    part = parameters.getXML().get("part");

    if( parameters.getXML().containsKey("mappingDataStream") ){
      String mappingDataStreamName = parameters.getXML().get("mappingDataStream", "");
      TypeReader mappingDataStream = parameters.getTypeReader(mappingDataStreamName);
      mappingData = new DocumentMappingReader( mappingDataStream );
    }
  }

  public void process(DocumentSplit index) throws IOException {
    StructuredIndexPartReader reader = StructuredIndex.openIndexPart(index.fileName + File.separator + part);

    if(mergerClassName == null){
      mergerClassName = reader.getManifest().get("mergerClass");
      writerClassName = reader.getManifest().get("writerClass");
    } else {
      assert( mergerClassName.equals(reader.getManifest().get("mergerClass")) ) : "mergeClass attributes are inconsistent." ;
      assert( writerClassName.equals(reader.getManifest().get("writerClass")) ) : "writerClass attributes are inconsistent." ;
    }

    indexPartReaders.put(reader, index.fileId);
  }

  public void close() throws IOException {

    try {
      parameters.getXML().add("writerClass",writerClassName);
      Class m = Class.forName(mergerClassName);
      Constructor c = m.getConstructor( TupleFlowParameters.class );

      System.err.println( "GOT CONSTRUCTOR. " );
      GenericIndexMerger merger = (GenericIndexMerger) c.newInstance( parameters );
      System.err.println( "GOT INSTANCE. " );

      merger.setDocumentMapping( mappingData );
      merger.setInputs( indexPartReaders );
      merger.performKeyMerge();
      merger.close();

    } catch (Exception ex) {
      Logger.getLogger(IndexPartMergeManager.class.getName()).log(Level.SEVERE, "Errored Merging Part: " + part, ex);
      throw new IOException(ex);
    }
  }
}
