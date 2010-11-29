/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.index;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;

import junit.framework.TestCase;

import org.galagosearch.core.types.DataMapItem;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DataMapTest extends TestCase {
    public DataMapTest(String testName) {
        super(testName);
    }

    public void testWriter() throws Exception {
      
      Parameters p = new Parameters();
      File f = Utility.createTemporary();
      f.delete();
      f.mkdirs();
      
      DataMapWriter writer = new DataMapWriter( f.getAbsoluteFile(), "test" );
      
      ArrayList<DataMapItem> data = new ArrayList();
      for(int key_val = 10 ; key_val < 25 ; key_val++){
        String str_val = "key is " + key_val;
        byte[] key = Utility.fromInt(key_val);
        byte[] value = Utility.fromString(str_val);
        DataMapItem dat = new DataMapItem(key, value);
        data.add(dat);
      }
      Collections.sort(data, (new DataMapItem.KeyOrder().lessThan()));
      for(DataMapItem d : data){
        writer.process(d);
      }
      writer.close();
      
      DataMapReader reader = new DataMapReader( f.getAbsolutePath(), "test", false );

      DataMapItem result = null;

      result = reader.get(Utility.fromInt(11));
      assertNotNull(result);
      assertEquals(11, Utility.toInt(result.key));

      String str_val = "key is " + 11;
      assertTrue(str_val.equals(Utility.toString(result.value)));

      Utility.deleteDirectory(f);
    } 
} 
  