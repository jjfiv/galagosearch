package org.galagosearch.core.window;

import gnu.trove.TObjectIntHashMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.security.NoSuchAlgorithmException;
import org.galagosearch.core.parse.*;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.NullProcessor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import junit.framework.*;
import org.galagosearch.core.types.TextFeature;
import org.galagosearch.tupleflow.FakeParameters;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class WindowFeaturerTest extends TestCase {

  public WindowFeaturerTest(String testName) {
    super(testName);
  }

  public void testOrderedWindowFeaturer() throws IOException, NoSuchAlgorithmException, IncompatibleProcessorException{

    Catcher<TextFeature> catcher = new Catcher();

    // first try bi-grams ~(#od:1(a b))
    WindowFeaturer featurer = new WindowFeaturer(new FakeParameters(new Parameters()));
    featurer.setProcessor( catcher );
    
    for(int i =0 ; i < 1000 ; i++){
      featurer.process( new Window( 0,i,1,2,3, Utility.fromString("word-"+i)) );
    }

    TObjectIntHashMap<byte[]> collisions = new TObjectIntHashMap();
    for( TextFeature t : catcher.data ){
      collisions.adjustOrPutValue(t.feature, 1, 1);
    }

    int c = 0;
    for( int val : collisions.getValues() ){
      if(val > 1){
        c++;
      }
    }

    assert (c == 0);
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
