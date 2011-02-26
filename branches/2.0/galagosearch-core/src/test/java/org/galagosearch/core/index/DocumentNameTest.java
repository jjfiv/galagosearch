/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.index;

import java.io.File;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.FakeParameters;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DocumentNameTest extends TestCase {
    public DocumentNameTest(String testName) {
        super(testName);
    }

    public void testDocumentNameStore() throws Exception {
      
      Parameters p = new Parameters();
      File f = Utility.createTemporary();
      f.delete();
      f.mkdir();
      File names = File.createTempFile("docName.", "", f);
      p.add("filename", names.getAbsolutePath());
      FakeParameters params = new FakeParameters(p);
      DocumentNameWriter writer = new DocumentNameWriter(params);
            
      for(int key_val = 10 ; key_val < 35 ; key_val++){
        String str_val = "document_name_key_is_" + key_val;
        NumberedDocumentData ndd = new NumberedDocumentData(str_val, "", key_val, 0);
        writer.process(ndd);
      }
      writer.close();
      
      DocumentNameReader reader = new DocumentNameReader(names.getAbsolutePath());
      DocumentNameReader revReader = new DocumentNameReader(names.getAbsolutePath()+".reverse");

      int key = 15;
      String name = "document_name_key_is_" + key;
      
      String result1 = reader.get(key);
      assert name.equals(result1);
      
      int result2 = revReader.getDocumentId(name);
      assert key == result2;

      Utility.deleteDirectory(f);
    } 
} 
  