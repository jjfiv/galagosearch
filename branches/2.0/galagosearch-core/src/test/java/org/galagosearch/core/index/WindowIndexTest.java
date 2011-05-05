/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.index;

import java.io.File;
import junit.framework.TestCase;
import org.galagosearch.core.index.WindowIndexReader.KeyIterator;
import org.galagosearch.core.index.WindowIndexReader.TermExtentIterator;
import org.galagosearch.tupleflow.FakeParameters;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class WindowIndexTest extends TestCase {

  public WindowIndexTest(String name) {
    super(name);
  }

  public void testWindowIndex() throws Exception {
    File index = Utility.createTemporary();
    try {
      Parameters p = new Parameters();
      p.add("filename", index.getAbsolutePath());
      WindowIndexWriter writer = new WindowIndexWriter(new FakeParameters(p));

      writer.processExtentName(Utility.fromString("word1"));
      for (int doc = 0; doc < 10; doc += 2) {
        writer.processNumber(doc);
        for (int begin = 0; begin < 21; begin += 5) {
          writer.processBegin(begin);
          writer.processTuple(begin + 1);
          writer.processTuple(begin + 2);

          //System.err.println("word1\t"+doc+"\t"+begin + "\t" + (begin+1) );
          //System.err.println("word1\t"+doc+"\t"+begin + "\t" + (begin+2) );
        }
      }
      writer.processExtentName(Utility.fromString("word2"));
      for (int doc = 0; doc < 10; doc += 2) {
        writer.processNumber(doc);
        for (int begin = 0; begin < 21; begin += 5) {
          writer.processBegin(begin);
          writer.processTuple(begin + 1);
          writer.processTuple(begin + 2);

          //System.err.println("word1\t"+doc+"\t"+begin + "\t" + (begin+1) );
          //System.err.println("word1\t"+doc+"\t"+begin + "\t" + (begin+2) );
        }
      }

      writer.close();

      WindowIndexReader reader = new WindowIndexReader(index.getAbsolutePath());
      KeyIterator iterator = reader.getIterator();
      while (!iterator.isDone()) {
        iterator.getValueIterator();
        WindowIndexReader.TermExtentIterator valueIterator = (TermExtentIterator) iterator.getValueIterator();
        int doccount = 0;
        int windowcount = 0;
        while( !valueIterator.isDone() ){
          doccount++;
          windowcount += valueIterator.extents().getPositionCount();
          valueIterator.next();
        }
        assert doccount == 5;
        assert windowcount == 50;

        iterator.nextKey();
      }

    } finally {
      index.delete();
    }
  }
}
