/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.index.merge;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import junit.framework.TestCase;
import org.galagosearch.core.index.DocumentNameWriter;
import org.galagosearch.core.types.DocumentMappingData;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.FakeParameters;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DocumentNumberMapperTest extends TestCase {

  public DocumentNumberMapperTest(String name) {
    super(name);
  }

  private void makeNamesIndex(int maxDocNum, File folder) throws Exception {
    File temp = new File(folder + File.separator + "names");
    Parameters p = new Parameters();
    p.set("filename", temp.getAbsolutePath());
    DocumentNameWriter writer = new DocumentNameWriter(new FakeParameters(p));

    for (int i = 0; i <= maxDocNum; i++) {
      writer.process(new NumberedDocumentData("doc-"+i, "", i, 0));
    }

    writer.close();
  }


  public void testDocumentMappingCreator() throws Exception {

    File index1 = null;
    File index2 = null;
    File index3 = null;
    try {
      index1 = Utility.createGalagoTempDir();
      index2 = Utility.createGalagoTempDir();
      index3 = Utility.createGalagoTempDir();

      // three 10 document indexes (0 -> 9)
      makeNamesIndex(9, index1);
      makeNamesIndex(9, index2);
      makeNamesIndex(9, index3);

      Catcher<DocumentMappingData> catcher = new Catcher();
      DocumentNumberMapper mapper = new DocumentNumberMapper();
      mapper.setProcessor( catcher );

      mapper.process( new DocumentSplit(index1.getAbsolutePath(), "", false, new byte[0],new byte[0],0,3 ));
      mapper.process( new DocumentSplit(index2.getAbsolutePath(), "", false, new byte[0],new byte[0],1,3 ));
      mapper.process( new DocumentSplit(index3.getAbsolutePath(), "", false, new byte[0],new byte[0],2,3 ));

      mapper.close();

      // for(DocumentMappingData d : catcher.data){
      //  System.err.println(d.toString());
      // }

      assert( catcher.data.get(0).indexId == 0 &&  catcher.data.get(0).docNumIncrement == 0);
      assert( catcher.data.get(1).indexId == 1 &&  catcher.data.get(1).docNumIncrement == 10);
      assert( catcher.data.get(2).indexId == 2 &&  catcher.data.get(2).docNumIncrement == 20);

    } finally {
      if (index1 != null) {
        Utility.deleteDirectory(index1);
      }
      if (index2 != null) {
        Utility.deleteDirectory(index2);
      }
      if (index3 != null) {
        Utility.deleteDirectory(index3);
      }
    }
  }

  public class Catcher<T> implements Processor<T> {

    ArrayList<T> data = new ArrayList();

    public void reset(){
      data = new ArrayList();
    }

    public void process(T object) throws IOException {
      data.add(object);
    }

    public void close() throws IOException {
      //nothing
    }
  }

}
