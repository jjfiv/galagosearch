
package org.galagosearch.core.retrieval.structured;

import java.io.File;
import junit.framework.TestCase;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.tools.App;
import org.galagosearch.core.tools.AppTest;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author irmarc
 */
public class IndicatorIteratorTest extends TestCase {

    File relsFile = null;
    File queryFile = null;
    File scoresFile = null;
    File trecCorpusFile = null;
    File corpusFile = null;
    File indexFile = null;

    public IndicatorIteratorTest(String testName) {
        super(testName);
    }

    // Build an index based on 10 short docs
    @Override
    public void setUp() throws Exception {

        // create a simple doc file, trec format:
        StringBuilder trecCorpus = new StringBuilder();
        trecCorpus.append(AppTest.trecDocument("0", "This is a sample document"));
        trecCorpus.append(AppTest.trecDocument("1", "If the shoe fits, it's ugly"));
        trecCorpus.append(AppTest.trecDocument("2", "The cat jumped over third sample document"));
        trecCorpus.append(AppTest.trecDocument("3", "To be trusted is a greater compliment than to be loved"));
        trecCorpus.append(AppTest.trecDocument("4", "Though a sample program be but three lines long, someday it will have to be maintained via document."));
        trecCorpusFile = File.createTempFile("galago", ".trectext");
        Utility.copyStringToFile(trecCorpus.toString(), trecCorpusFile);

        // now, attempt to make a corpus file from that.
        corpusFile = File.createTempFile("galago", ".corpus");
        corpusFile.delete();
        App.main(new String[]{"make-corpus", corpusFile.getAbsolutePath(),
                    trecCorpusFile.getAbsolutePath()});


        // make sure the corpus file exists
        assertTrue(corpusFile.exists());

        // now, try to build an index from that
        indexFile = Utility.createTemporary();
        indexFile.delete();
        App.main(new String[]{"build", "--stemming=false", indexFile.getAbsolutePath(),
                    corpusFile.getAbsolutePath()});

        AppTest.verifyIndexStructures(indexFile);

        // optional printing of various stats
        App runner = new App(System.out);
    }

    @Override
    public void tearDown() throws Exception {
        if (relsFile != null) {
            relsFile.delete();
        }
        if (queryFile != null) {
            queryFile.delete();
        }
        if (scoresFile != null) {
            scoresFile.delete();
        }
        if (trecCorpusFile != null) {
            trecCorpusFile.delete();
        }
        if (corpusFile != null) {
            Utility.deleteDirectory(corpusFile);
        }
        if (indexFile != null) {
            Utility.deleteDirectory(indexFile);
        }
    }

    public void testExistentialIndicator() throws Exception {
              // Create a retrieval object for use by the traversal
        Parameters p = new Parameters();
        p.add("retrievalGroup", "all");
        p.set("index", indexFile.getAbsolutePath());
        StructuredRetrieval retrieval = (StructuredRetrieval) RetrievalFactory.instance(p);

        Node parsedTree = StructuredQuery.parse("#any( #counts:cat:part=postings() #counts:program:part=postings() )");
        DocumentContext context = new DocumentContext();
        ExistentialIndicatorIterator eii = (ExistentialIndicatorIterator) retrieval.createIterator(parsedTree, context);

        // initial state
        assertEquals(2, eii.currentCandidate());
        context.document = 2;
        assertTrue(eii.hasMatch(2));
        assertEquals(1, eii.getIndicatorStatus());

        assertFalse(eii.moveTo(3));
        context.document = 3;
        assertFalse(eii.getStatus());
        assertEquals(4, eii.currentCandidate());
        context.document = 4;
        assertTrue(eii.getStatus());

        assertFalse(eii.next());
        assertTrue(eii.isDone());

        retrieval.close();
    }

    public void testUniversalIndicator() throws Exception {
              // Create a retrieval object for use by the traversal
        Parameters p = new Parameters();
        p.add("retrievalGroup", "all");
        p.set("index", indexFile.getAbsolutePath());
        StructuredRetrieval retrieval = (StructuredRetrieval) RetrievalFactory.instance(p);

        Node parsedTree = StructuredQuery.parse("#all( #counts:document:part=postings() #counts:sample:part=postings() )");
        DocumentContext context = new DocumentContext();
        UniversalIndicatorIterator uii = (UniversalIndicatorIterator) retrieval.createIterator(parsedTree, context);

        // initial state
        assertEquals(0, uii.currentCandidate());
        context.document = 0;
        assertTrue(uii.hasMatch(0));
        assertEquals(1, uii.getIndicatorStatus());

        assertFalse(uii.moveTo(1));
        context.document = 1;
        assertFalse(uii.getStatus());
        assertEquals(2, uii.currentCandidate());
        context.document = 2;
        assertTrue(uii.getStatus());

        assertTrue(uii.next());
        assertEquals(4, uii.currentCandidate());
        assertFalse(uii.next());
        assertTrue(uii.isDone());

        retrieval.close();
    }
}
