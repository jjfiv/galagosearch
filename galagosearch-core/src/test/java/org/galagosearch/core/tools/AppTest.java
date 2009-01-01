/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.tools;

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

    /**
     * Test of getSplitStage method, of class App.
     */
    public void testGetSplitStage() {
        App app = new App();
        Job job = new Job();
        job.add(app.getSplitStage("fake-filename"));
        ErrorStore store = new ErrorStore();
        
        Verification.verify(job, store);
        assertEquals(0, store.getErrors().size());
        assertEquals(0, store.getWarnings().size());
    }

    /**
     * Test of getParseStage method, of class App.
     */
    public void testGetParseStage() {
        App app = new App();
        Job job = new Job();
        job.add(app.getParsePostingsStage());
        ErrorStore store = new ErrorStore();
        
        Verification.verify(job, store);
        assertEquals(0, store.getErrors().size());
        assertEquals(0, store.getWarnings().size());
    }

    /**
     * Test of getLinkCombineStage method, of class App.
     */
    public void testGetLinkCombineStage() {
        App app = new App();
        Job job = new Job();
        job.add(app.getLinkCombineStage());
        ErrorStore store = new ErrorStore();
        
        Verification.verify(job, store);
        assertEquals(0, store.getErrors().size());
        assertEquals(0, store.getWarnings().size());
    }

    /**
     * Test of getWritePostingsStage method, of class App.
     */
    public void testGetWritePostingsStage() {
        App app = new App();
        Job job = new Job();
        job.add(app.getWritePostingsStage());
        ErrorStore store = new ErrorStore();
        
        Verification.verify(job, store);
        assertEquals(0, store.getErrors().size());
        assertEquals(0, store.getWarnings().size());
    }

    /**
     * Test of getWriteExtentsStage method, of class App.
     */
    public void testGetWriteExtentsStage() {
        App app = new App();
        Job job = new Job();
        job.add(app.getWriteExtentsStage());
        ErrorStore store = new ErrorStore();
        
        Verification.verify(job, store);
        assertEquals(0, store.getErrors().size());
        assertEquals(0, store.getWarnings().size());
    }

    /**
     * Test of getWriteDatesStage method, of class App.
     */
    public void testGetWriteDatesStage() {
        App app = new App();
        Job job = new Job();
        job.add(app.getWriteDatesStage());
        ErrorStore store = new ErrorStore();
        
        Verification.verify(job, store);
        assertEquals(0, store.getErrors().size());
        assertEquals(0, store.getWarnings().size());
    }

    /**
     * Test of getWriteManifestStage method, of class App.
     */
    public void testGetWriteManifestStage() {
        App app = new App();
        Job job = new Job();
        job.add(app.getWriteManifestStage());
        ErrorStore store = new ErrorStore();
        
        Verification.verify(job, store);
        assertEquals(0, store.getErrors().size());
        assertEquals(0, store.getWarnings().size());
    }

    /**
     * Test of getWriteDocumentLengthsStage method, of class App.
     */
    public void testGetWriteDocumentLengthsStage() {
        App app = new App();
        Job job = new Job();
        job.add(app.getWriteDocumentLengthsStage());
        ErrorStore store = new ErrorStore();
        
        Verification.verify(job, store);
        assertEquals(0, store.getErrors().size());
        assertEquals(0, store.getWarnings().size());
    }

    /**
     * Test of getIndexJob method, of class App.
     */
    public void testGetIndexJob() {
        App app = new App();
        Job job = app.getIndexJob("one", "two");
        ErrorStore store = new ErrorStore();
        
        Verification.verify(job, store);
        assertEquals("", store.toString());
        assertEquals(0, store.getErrors().size());
        assertEquals(0, store.getWarnings().size());
    }
}
