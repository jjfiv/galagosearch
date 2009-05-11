// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.tools;

import java.io.IOException;
import junit.framework.TestCase;
import org.galagosearch.tupleflow.execution.ErrorStore;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.execution.Verification;

/**
 *
 * @author trevor
 */
public class AppTest extends TestCase {
    
    public AppTest(String testName) {
        super(testName);
    }

    public void testUsage() throws IOException {
        Job job = App.getDocumentConverter("in", new String[] {"/"});
        ErrorStore store = new ErrorStore();

        Verification.verify(job, store);
        assertEquals("", store.toString());
    }
}
