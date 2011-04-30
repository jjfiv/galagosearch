package org.galagosearch.core.window;

import org.galagosearch.core.parse.*;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.NullProcessor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import junit.framework.*;
import org.galagosearch.tupleflow.FakeParameters;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class WindowProducerTest extends TestCase {

  public WindowProducerTest(String testName) {
    super(testName);
  }

  public void testOrderedWindowProduction() throws IOException, IncompatibleProcessorException {
    // Document:
    NumberedDocument doc1 = new NumberedDocument();
    doc1.fileId = 1;
    doc1.number = 10;
    doc1.terms = Arrays.asList(new String[]{"1","9","2","8","3","7","4","6","5"});
              // "5","6","4","7","3","8","2","9","1"} );

   NumberedDocument doc2 = new NumberedDocument();
    doc2.fileId = 2;
    doc2.number = 10;
    doc2.terms = Arrays.asList(new String[]{"5","6","4","7","3","8","2","9","1"} );
    
    Catcher<Window> catcher = new Catcher();

    // first try bi-grams ~(#od:1(a b))
    Parameters p = new Parameters();
    p.add( "n", "2" );
    p.add( "width", "2" );
    p.add( "ordered", "true" );
    WindowProducer bigramProducer = new WindowProducer(new FakeParameters(p));
    bigramProducer.setProcessor( catcher );

    bigramProducer.process( doc1 );

    assert( catcher.data.size() == 15 );
    assert( Utility.toString( catcher.data.get(0).data ).equals("1~9") );
    assert( Utility.toString( catcher.data.get(1).data ).equals("1~2") );
    assert( Utility.toString( catcher.data.get(2).data ).equals("9~2") );
    assert( Utility.toString( catcher.data.get(3).data ).equals("9~8") );

    assert( catcher.data.get(4).document == 10 );
    assert( catcher.data.get(5).document == 10 );

    assert( catcher.data.get(6).begin == 3 );
    assert( catcher.data.get(6).end == 4 );

    assert( catcher.data.get(7).begin == 3 );
    assert( catcher.data.get(7).end == 5 );


    assert( catcher.data.get(9).file == 1 );
    assert( catcher.data.get(10).filePosition == 10 );

    bigramProducer.process( doc2 );

    assert( catcher.data.get(17).file == 2 );
    assert( catcher.data.get(18).filePosition == 3 );

  }

  public void testUnorderedWindowProduction() throws IOException, IncompatibleProcessorException {
    // Document:
    NumberedDocument doc = new NumberedDocument();
    doc.number = 10;
    doc.terms = Arrays.asList(new String[]{"1","9","2","8","3","7","4","6","5"});
              // "5","6","4","7","3","8","2","9","1"} );

    Catcher<Window> catcher = new Catcher();

    // first try bi-grams ~(#od:1(a b))
    Parameters p = new Parameters();
    p.add( "n", "3" );
    p.add( "width", "5" );
    p.add( "ordered", "false" );
    WindowProducer bigramProducer = new WindowProducer(new FakeParameters(p));
    bigramProducer.setProcessor( catcher );

    bigramProducer.process( doc );

    assert( catcher.data.size() == 34 );
    assert( Utility.toString( catcher.data.get(0).data ).equals("1~2~9") );
    assert( Utility.toString( catcher.data.get(1).data ).equals("1~8~9") );
    assert( Utility.toString( catcher.data.get(10).data ).equals("7~8~9") );
    assert( Utility.toString( catcher.data.get(12).data ).equals("2~3~8") );

    assert( catcher.data.get(4).document == 10 );
    assert( catcher.data.get(5).document == 10 );

    assert( catcher.data.get(23).begin == 3 );
    assert( catcher.data.get(23).end == 7 );

    assert( catcher.data.get(27).begin == 4 );
    assert( catcher.data.get(27).end == 7 );

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
