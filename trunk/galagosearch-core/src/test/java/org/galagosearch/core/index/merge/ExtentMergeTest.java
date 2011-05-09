/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.index.merge;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import junit.framework.TestCase;
import org.galagosearch.core.index.ExtentIndexReader;
import org.galagosearch.core.index.ExtentIndexReader.KeyIterator;
import org.galagosearch.core.index.ExtentIndexWriter;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.index.StructuredIndexPartReader;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.core.types.NumberedExtent;
import org.galagosearch.tupleflow.FakeParameters;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class ExtentMergeTest extends TestCase {

  public ExtentMergeTest(String name) {
    super(name);
  }

  private void makeExtentsIndex(int offset, File folder) throws Exception {
    File temp = new File(folder + File.separator + "extents");
    Parameters p = new Parameters();
    p.set("filename", temp.getAbsolutePath());
    ExtentIndexWriter writer = new ExtentIndexWriter(new FakeParameters(p));

    for(String word : new String[]{"word1","word2"}) {
      writer.processExtentName(Utility.fromString(word));
      for(int doc = offset ; doc < offset+50 ; doc+=10) { // 100, 110, 120...
        writer.processNumber(doc);
        for(int begin = offset+5 ; begin < offset+50 ; begin+=10){ //105, 115, 125...
          writer.processBegin(begin);
          writer.processTuple(begin + 1); // 106 116 126
          writer.processTuple(begin + 2); // 107 117 127
        }
      }
    }

    writer.close();
  }


  public void testExtentIndexMerger() throws Exception {

    File index1 = null;
    File index2 = null;
    File index3 = null;
    File output = null;
    try {
      index1 = Utility.createGalagoTempDir();
      index2 = Utility.createGalagoTempDir();
      index3 = Utility.createGalagoTempDir();
      output = Utility.createTemporary();

      // three 10 document indexes (0 -> 9)
      makeExtentsIndex(100, index1);
      makeExtentsIndex(200, index2);
      makeExtentsIndex(300, index3);

      Parameters p = new Parameters();
      p.add("filename", output.getAbsolutePath());
      p.add("writerClass", ExtentIndexWriter.class.getName());
      /*ExtentIndexMerger merger = new ExtentIndexMerger(new FakeParameters(p));

      merger.setDocumentMapping(null);

      HashMap<StructuredIndexPartReader, Integer> inputs = new HashMap();
      inputs.put( StructuredIndex.openIndexPart(index1.getAbsolutePath()) , 1);
      inputs.put( StructuredIndex.openIndexPart(index2.getAbsolutePath()) , 2);
      inputs.put( StructuredIndex.openIndexPart(index3.getAbsolutePath()) , 3);
      merger.setInputs( inputs );
      merger.performKeyMerge();
      merger.close();

      // testing
      ExtentIndexReader reader = (ExtentIndexReader) StructuredIndex.openIndexPart(index3.getAbsolutePath());
      KeyIterator iterator = reader.getIterator();
      while(!iterator.isDone()){
        System.err.println( iterator.getKey() );

      }
       *
       */

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
      if (output != null) {
        Utility.deleteDirectory(output);
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
