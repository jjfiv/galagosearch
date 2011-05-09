
package org.galagosearch.core.parse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import junit.framework.TestCase;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.core.types.DocumentSplit.Processor;
import org.galagosearch.tupleflow.FakeParameters;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class DocumentSourceTest extends TestCase {
    
    public DocumentSourceTest(String testName) {
        super(testName);
    }

    class FakeProcessor implements Processor {
        public ArrayList<DocumentSplit> splits = new ArrayList<DocumentSplit>();

        public void process(DocumentSplit split) {
            splits.add(split);
        }
        public void close() throws IOException {}
    }

    public void testUnknownFile() throws Exception {
        Parameters p = new Parameters();
        p.add("filename", "foo.c");
        DocumentSource source = new DocumentSource(new FakeParameters(p));
        FakeProcessor processor = new FakeProcessor();
        source.setProcessor(processor);

        boolean threwException = false;
        try {
            source.run();
        } catch(Exception e) {
            threwException = true;
        }
        assertTrue(threwException);
    }

    public void testUnknownExtension() throws Exception {
        File tempFile = Utility.createTemporary();
        Parameters p = new Parameters();
        p.add("filename", tempFile.getAbsolutePath());
        DocumentSource source = new DocumentSource(new FakeParameters(p));
        FakeProcessor processor = new FakeProcessor();
        source.setProcessor(processor);

        source.run();
        assertEquals(0, processor.splits.size());
        tempFile.delete();
    }
}
