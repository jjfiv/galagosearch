// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import junit.framework.TestCase;
import org.galagosearch.core.parse.DocumentSource;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.execution.ErrorStore;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.execution.Verification;

/**
 *
 * @author trevor
 */
public class BuildIndexTest extends TestCase {

  public BuildIndexTest(String testName) {
    super(testName);
  }

  public void testCollectionLengthStage() throws IOException {
    BuildIndex buildIndex = new BuildIndex();
    Job job = new Job();
    job.add(BuildStageTemplates.getCollectionLengthStage("collectionLength", "numberedDocumentData", "collectionLength"));
    ErrorStore store = new ErrorStore();

    Verification.verify(job, store);
    assertEquals(0, store.getErrors().size());
    assertEquals(0, store.getWarnings().size());
  }

  /**
   * Test of getSplitStage method, of class BuildIndex.
   */
  public void testGetSplitStage() throws IOException {
    Job job = new Job();
    ArrayList<String> d = new ArrayList();
    d.add("/");
    job.add(BuildStageTemplates.getSplitStage(d, DocumentSource.class));
    ErrorStore store = new ErrorStore();

    Verification.verify(job, store);
    assertEquals(0, store.getErrors().size());
    assertEquals(0, store.getWarnings().size());
  }

  /**
   * Test of getParseStage method, of class BuildIndex.
   */
  public void testGetParseStage() {
    BuildIndex BuildIndex = new BuildIndex();
    Job job = new Job();
    job.add(BuildIndex.getParsePostingsStage());
    ErrorStore store = new ErrorStore();

    Verification.verify(job, store);
    assertEquals(0, store.getErrors().size());
    assertEquals(0, store.getWarnings().size());
  }

  /**
   * Test of getLinkCombineStage method, of class BuildIndex.
   */
  public void testGetLinkCombineStage() {
    BuildIndex BuildIndex = new BuildIndex();
    Job job = new Job();
    job.add(BuildIndex.getLinkCombineStage());
    ErrorStore store = new ErrorStore();

    Verification.verify(job, store);
    assertEquals("", store.toString());
  }

  /**
   * Test of getWritePostingsStage method, of class BuildIndex.
   */
  public void testGetWritePostingsStage() {
    BuildIndex BuildIndex = new BuildIndex();
    Job job = new Job();
    job.add(BuildIndex.getWritePostingsStage("postings", "input", "index"));
    ErrorStore store = new ErrorStore();

    Verification.verify(job, store);
    assertEquals(0, store.getErrors().size());
    assertEquals(0, store.getWarnings().size());
  }

  /**
   * Test of getWriteExtentsStage method, of class BuildIndex.
   */
  public void testGetWriteExtentsStage() throws IOException {
    Job job = new Job();
    job.add(BuildStageTemplates.getWriteExtentsStage("writeExtents", new File("./extents"), "numberedExtents"));
    ErrorStore store = new ErrorStore();

    Verification.verify(job, store);
    assertEquals(0, store.getErrors().size());
    assertEquals(0, store.getWarnings().size());
  }

  /**
   * Test of getWriteDatesStage method, of class BuildIndex.
   */
  public void testGetWriteDatesStage() throws IOException {
    Job job = new Job();
    job.add(BuildStageTemplates.getWriteDatesStage("writeDates", new File("./dates"), "numberedExtents"));
    ErrorStore store = new ErrorStore();

    Verification.verify(job, store);
    assertEquals(0, store.getErrors().size());
    assertEquals(0, store.getWarnings().size());
  }

  /**
   * Test of getWriteManifestStage method, of class BuildIndex.
   */
  public void testGetWriteManifestStage() throws IOException {
    Job job = new Job();
    job.add(BuildStageTemplates.getWriteManifestStage("writeManifest", new File("./manifest"), "collectionLength"));
    ErrorStore store = new ErrorStore();

    Verification.verify(job, store);
    assertEquals(0, store.getErrors().size());
    assertEquals(0, store.getWarnings().size());
  }

  /**
   * Test of getWriteDocumentLengthsStage method, of class BuildIndex.
   */
  public void testGetWriteDocumentLengthsStage() throws IOException {
    Job job = new Job();
    job.add(BuildStageTemplates.getWriteLengthsStage("writeLengths", new File("./lengths"), "collectionLengths"));
    ErrorStore store = new ErrorStore();

    Verification.verify(job, store);
    assertEquals(0, store.getErrors().size());
    assertEquals(0, store.getWarnings().size());
  }

  /**
   * Test of getIndexJob method, of class BuildIndex.
   */
  public void testGetIndexJob() throws IOException {
    BuildIndex buildIndex = new BuildIndex();

    Parameters p = new Parameters();
    p.add("indexPath", "one");
    p.add("inputPath", "/");
    p.add("links", "false");
    p.add("stemming", "true");
    Job job = buildIndex.getIndexJob(p);
    ErrorStore store = new ErrorStore();

    Verification.verify(job, store);
    assertEquals("", store.toString());
  }

  public void testJobWithStemming() throws IOException {
    BuildIndex buildIndex = new BuildIndex();

    Parameters p = new Parameters();
    p.add("indexPath", "one");
    p.add("inputPath", "/");
    p.add("links", "false");
    p.add("stemming", "true");
    Job job = buildIndex.getIndexJob(p);
    ErrorStore store = new ErrorStore();

    Verification.verify(job, store);
    assertEquals("", store.toString());
  }

  public void testJobWithLinks() throws IOException {
    BuildIndex buildIndex = new BuildIndex();
    Parameters p = new Parameters();
    p.add("indexPath", "one");
    p.add("inputPath", "/");
    p.add("links", "true");
    p.add("stemming", "false");
    Job job = buildIndex.getIndexJob(p);

    ErrorStore store = new ErrorStore();

    Verification.verify(job, store);
    assertEquals("", store.toString());
  }

  public void testJobWithLinksAndStemming() throws IOException {
    BuildIndex buildIndex = new BuildIndex();
    Parameters p = new Parameters();
    p.add("indexPath", "one");
    p.add("inputPath", "/");
    p.add("links", "true");
    p.add("stemming", "true");
    Job job = buildIndex.getIndexJob(p);

    ErrorStore store = new ErrorStore();

    Verification.verify(job, store);
    assertEquals("", store.toString());
  }
}
