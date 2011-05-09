/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.index.merge;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Random;
import junit.framework.TestCase;
import org.galagosearch.core.index.DocumentLengthsReader;
import org.galagosearch.core.index.DocumentLengthsReader.KeyIterator;
import org.galagosearch.core.index.DocumentLengthsWriter;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.index.StructuredIndexPartReader;
import org.galagosearch.core.tools.App;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.FakeParameters;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DocumentLengthsMergerTest extends TestCase {

  public DocumentLengthsMergerTest(String testName) {
    super(testName);
  }

  private String makeLengthsIndex(int firstDocNum, File folder) throws Exception {
    File temp = new File(folder + File.separator + "lengths");
    Parameters p = new Parameters();
    p.set("filename", temp.getAbsolutePath());
    DocumentLengthsWriter writer = new DocumentLengthsWriter(new FakeParameters(p));

    for (int i = firstDocNum; i < firstDocNum + 100; i++) {
      writer.process(new NumberedDocumentData("", "", i, i + 1));
    }

    writer.close();

    return temp.getAbsolutePath();
  }

  public void testMerge1() throws Exception {
    // make two or three doclengths files

    File folder1 = null;
    File folder2 = null;
    String output = null;

    try {

      folder1 = Utility.createGalagoTempDir();
      folder2 = Utility.createGalagoTempDir();
      output = Utility.createTemporary().getAbsolutePath();

      String index1 = makeLengthsIndex(0, folder1);
      String index2 = makeLengthsIndex(100, folder2);

      StructuredIndexPartReader reader1 = StructuredIndex.openIndexPart(index1);
      StructuredIndexPartReader reader2 = StructuredIndex.openIndexPart(index2);

      HashMap<StructuredIndexPartReader, Integer> indexPartReaders = new HashMap();
      indexPartReaders.put(reader1, 1);
      indexPartReaders.put(reader2, 2);

      String writerClassName = reader1.getManifest().get("writerClass");
      String mergeClassName = reader1.getManifest().get("mergerClass");

      Parameters p = new Parameters();
      p.add("writerClass", writerClassName);
      p.add("filename", output);

      Class m = Class.forName(mergeClassName);
      Constructor c = m.getConstructor(TupleFlowParameters.class);
      GenericIndexMerger merger = (GenericIndexMerger) c.newInstance(new FakeParameters(p));

      merger.setDocumentMapping(null);
      merger.setInputs(indexPartReaders);
      merger.performKeyMerge();
      merger.close();

      // test that there are 100 keys and values.
      DocumentLengthsReader tester = new DocumentLengthsReader(output);
      KeyIterator iterator = tester.getIterator();
      while (!iterator.isDone()) {
        assert (iterator.getCurrentDocument() + 1 == iterator.getCurrentLength());
        iterator.nextKey();
      }

    } finally {

      if (folder1 != null) {
        Utility.deleteDirectory(folder1);
      }
      if (folder2 != null) {
        Utility.deleteDirectory(folder2);
      }
      if (output != null) {
        new File(output).delete();
      }
    }
  }

  public void testMerge2() throws Exception {
    // make two or three doclengths files
    File indexFolder1 = null;
    File indexFolder2 = null;
    String output = null;

    try {
      indexFolder1 = Utility.createGalagoTempDir();
      indexFolder2 = Utility.createGalagoTempDir();
      output = Utility.createTemporary().getAbsolutePath();

      String index1 = makeLengthsIndex(0, indexFolder1);
      String index2 = makeLengthsIndex(100, indexFolder2);

      Parameters p = new Parameters();
      p.add("part", "lengths");
      p.add("filename", output);
      IndexPartMergeManager manager = new IndexPartMergeManager(new FakeParameters(p));

      // add indexes to be merged
      manager.process(new DocumentSplit(indexFolder1.getAbsolutePath(), "", false, new byte[0], new byte[0], 2, 2));
      manager.process(new DocumentSplit(indexFolder2.getAbsolutePath(), "", false, new byte[0], new byte[0], 1, 2));

      // perform merge
      manager.close();

      // test that there are 100 keys and values.
      DocumentLengthsReader tester = new DocumentLengthsReader(output);
      KeyIterator iterator = tester.getIterator();
      while (!iterator.isDone()) {
        assert (iterator.getCurrentDocument() + 1 == iterator.getCurrentLength());
        iterator.nextKey();
      }


    } finally {

      if (indexFolder1 != null) {
        Utility.deleteDirectory(indexFolder1);
      }
      if (indexFolder2 != null) {
        Utility.deleteDirectory(indexFolder2);
      }
      if (output != null) {
        new File(output).delete();
      }
    }
  }
}
