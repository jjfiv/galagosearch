// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.mergeindex;

import org.galagosearch.core.index.KeyIterator;
import org.galagosearch.core.tools.*;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import junit.framework.TestCase;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class MergeIndexTest extends TestCase {

  public MergeIndexTest(String testName) {
    super(testName);
  }

  public String trecDocument(Random rnd) {
    return "<DOC>\n<DOCNO>docId-" + rnd.nextInt(50) + "</DOCNO>\n"
            + "<TEXT>\nstart " + rnd.nextInt(50) + " this is a test\n"
            + rnd.nextInt(50) + " and this is too\n"
            + rnd.nextInt(50) + " now we done with a test\n"
            + "</TEXT>\n</DOC>\n";
  }

  public File makeCorpusFile() throws Exception {
    Random rnd = new Random();
    String corpus = "";
    for (int i = 0; i < 10; i++) {
      corpus += trecDocument(rnd);
    }
    File trecCorpusFile = File.createTempFile("galago", ".trectext");
    Utility.copyStringToFile(corpus, trecCorpusFile);
    return trecCorpusFile;
  }

  public File makeIndex(List<File> trecCorpora) throws Exception {

    File indexFile = null;

    // try to build an index from the corpora files
    indexFile = Utility.createTemporary();
    indexFile.delete();
    ArrayList<String> args = new ArrayList();
    args.add("build-fast");
    args.add(indexFile.getAbsolutePath());
    for(File c : trecCorpora){
      args.add(c.getAbsolutePath());
    }

    App.main(args.toArray(new String[0]));

    // make sure the indexes exists
    assertTrue(indexFile.exists());

    return indexFile;
  }

  public void testMergeIndex() throws Exception {
    if (true) return; // mask this for now until we re-write properly

    File corpus1 = null;
    File corpus2 = null;
    File index1 = null;
    File index2 = null;
    File index12 = null;
    File indexmg = null;

    try {
      // make the corpus files
      corpus1 = makeCorpusFile();
      corpus2 = makeCorpusFile();
      ArrayList<File> corpus12 = new ArrayList();
      corpus12.add(corpus1);
      corpus12.add(corpus2);

      // make the indexes
      index1 = makeIndex(Collections.singletonList(corpus1));
      index2 = makeIndex(Collections.singletonList(corpus2));
      index12 = makeIndex(corpus12);
      
      // merge indexes 1 and 2
      indexmg = Utility.createTemporary();
      indexmg.delete();
      ArrayList<String> args = new ArrayList();
      args.add("merge-index");
      args.add(indexmg.getAbsolutePath());
      args.add(index1.getAbsolutePath());
      args.add(index2.getAbsolutePath());
      App.main(args.toArray(new String[0]));

      // now verify index12 and merged are similar
      StructuredIndex strctindex12 = new StructuredIndex(index12.getAbsolutePath());
      StructuredIndex strctindexmg = new StructuredIndex(indexmg.getAbsolutePath());

      // compare the manifests
      assertEquals(strctindex12.getCollectionLength(), strctindexmg.getCollectionLength());
      assertEquals(strctindex12.getDocumentCount(), strctindexmg.getDocumentCount());

      // compare the index keys
      IndexReader.Iterator iterator12 = new IndexReader(index12.getAbsolutePath() + File.separator + "parts" + File.separator + "postings").getIterator();
      IndexReader.Iterator iteratormg = new IndexReader(indexmg.getAbsolutePath() + File.separator + "parts" + File.separator + "postings").getIterator();
      while( ! iterator12.isDone() ){
        assert( Arrays.equals(iterator12.getKey(), iteratormg.getKey()) );
        iterator12.nextKey();
        iteratormg.nextKey();
      }

      iterator12 = new IndexReader(index12.getAbsolutePath() + File.separator + "parts" + File.separator + "stemmedPostings").getIterator();
      iteratormg = new IndexReader(indexmg.getAbsolutePath() + File.separator + "parts" + File.separator + "stemmedPostings").getIterator();
      while( ! iterator12.isDone() ){
        assert( Arrays.equals(iterator12.getKey(), iteratormg.getKey()) );
        iterator12.nextKey();
        iteratormg.nextKey();
      }

      // document numbers won't match so more checking needs to be carefully considered

      strctindex12.close();
      strctindexmg.close();


    } finally {
      if (corpus1 != null) {
        corpus1.delete();
      }
      if (corpus2 != null) {
        corpus2.delete();
      }
      if (index1 != null) {
        Utility.deleteDirectory(index1);
      }
      if (index2 != null) {
        Utility.deleteDirectory(index2);
      }
      if (index12 != null) {
        Utility.deleteDirectory(index12);
      }
      if (indexmg != null) {
        Utility.deleteDirectory(indexmg);
      }
    }
  }
}
