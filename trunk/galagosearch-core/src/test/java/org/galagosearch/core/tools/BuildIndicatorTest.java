// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.tools;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import junit.framework.TestCase;
import org.galagosearch.core.index.DocumentIndicatorReader;
import org.galagosearch.core.index.DocumentIndicatorReader.KeyIterator;
import org.galagosearch.core.index.DocumentNameReader;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class BuildIndicatorTest extends TestCase {

  public BuildIndicatorTest(String testName) {
    super(testName);
  }

  public static String trecDocument(String docno, String text) {
    return "<DOC>\n<DOCNO>d" + docno + "</DOCNO>\n"
            + "<TEXT>\n" + text + "</TEXT>\n</DOC>\n";
  }

  public void testIndicators() throws Exception {
    File trecCorpusFile = null;
    File indicatorFile = null;
    File indexFile = null;
    File queryFile = null;

    try {
      // create a simple doc file, trec format:
      String trecCorpus = trecDocument("55", "This is a sample document")
              + trecDocument("59", "sample document two")
              + trecDocument("73", "sample document three")
              + trecDocument("10", "sample document four")
              + trecDocument("11", "sample document five");
      trecCorpusFile = Utility.createTemporary();
      Utility.copyStringToFile(trecCorpus, trecCorpusFile);

      String indicators = "d1\n"
              + "d5\n"
              + "d55\ttrue\n"
              + "d59\tfalse\n"
              + "d10\n";

      indicatorFile = Utility.createTemporary();
      Utility.copyStringToFile(indicators, indicatorFile);


      // now, try to build an index from that
      indexFile = Utility.createTemporary();
      indexFile.delete();
      App.main(new String[]{"build", indexFile.getAbsolutePath(),
                trecCorpusFile.getAbsolutePath()});

      App.main(new String[]{"indicator", indexFile.getAbsolutePath(),
                indicatorFile.getAbsolutePath(), "--indicatorPart=testingIndicators"});

      DocumentIndicatorReader reader = new DocumentIndicatorReader(indexFile.getAbsolutePath() + File.separator + "testingIndicators");

      String output = "0	true\n"
              + "2	true\n"
              + "3	false\n";

      KeyIterator iterator = reader.getIterator();
      StringBuilder sb = new StringBuilder();
      do {
        sb.append(iterator.getCurrentDocument()).append("\t").append(iterator.getCurrentIndicator()).append("\n");
      } while (iterator.nextKey());

      assert output.equals(sb.toString());

      // now test a query:
      String queries =
              "<parameters>\n"
              + "<query><number>1</number><text>sample</text></query>\n"
              + "<query><number>2</number><text>#filter( #indicator:part=testingIndicators() sample )</text></query>\n"
              + "</parameters>\n";
      queryFile = Utility.createTemporary();
      Utility.copyStringToFile(queries, queryFile);

      // test with batch search
      ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
      PrintStream printStream = new PrintStream(byteArrayStream);

      new App(printStream).run(new String[]{"batch-search",
                "--index=" + indexFile.getAbsolutePath(),
                queryFile.getAbsolutePath()});

      // Now, verify that some stuff exists
      String out = byteArrayStream.toString();

      String expected = "1 Q0 d10 1 -1.22350933 galago\n"
              + "1 Q0 d11 2 -1.22350933 galago\n"
              + "1 Q0 d59 3 -1.22350933 galago\n"
              + "1 Q0 d73 4 -1.22350933 galago\n"
              + "1 Q0 d55 5 -1.22483912 galago\n"
              + "2 Q0 d10 1 -1.22350933 galago\n"
              + "2 Q0 d55 2 -1.22483912 galago\n";

      assertEquals (expected, out );

    } finally {
      if (trecCorpusFile != null) {
        trecCorpusFile.delete();
      }
      if (indicatorFile != null) {
        indicatorFile.delete();
      }
      if (queryFile != null) {
        queryFile.delete();
      }
      if (indexFile != null) {
        Utility.deleteDirectory(indexFile);
      }
    }
  }
}
