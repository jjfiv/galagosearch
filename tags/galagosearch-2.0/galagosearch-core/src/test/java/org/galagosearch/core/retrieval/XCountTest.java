package org.galagosearch.core.retrieval;

import org.galagosearch.core.retrieval.traversal.*;
import java.io.File;
import junit.framework.TestCase;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.retrieval.structured.CountIterator;
import org.galagosearch.core.retrieval.structured.ExtentConjunctionIterator;
import org.galagosearch.core.retrieval.structured.ExtentIterator;
import org.galagosearch.core.retrieval.structured.StructuredIterator;
import org.galagosearch.core.retrieval.structured.StructuredRetrieval;
import org.galagosearch.core.tools.App;
import org.galagosearch.core.tools.AppTest;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 * This test is seriously a pain so all traversals
 * that make use of 2 rounds of retrieval should use the testing
 * infrastructure set up here.
 *
 * If you want to print the various statistics, uncomment some of the
 * print calls below.
 *
 * TODO: Make stronger tests to increase confidence
 *
 * @author irmarc
 */
public class XCountTest extends TestCase {

    File trecCorpusFile = null;
    File indexFile = null;

    public XCountTest(String testName) {
        super(testName);
    }

    // Build an index based on 10 short docs
    @Override
    public void setUp() throws Exception {

        // create a simple doc file, trec format:
        StringBuilder trecCorpus = new StringBuilder();
        trecCorpus.append(AppTest.trecDocument("1", "This is <p>a sample</p> document"));
        trecCorpus.append(AppTest.trecDocument("2", "The a cat jumped over the moon"));
        trecCorpus.append(AppTest.trecDocument("3", "If the shoe fits, it's ugly"));
        trecCorpus.append(AppTest.trecDocument("4", "Though a program be but three lines long, someday it will have to be maintained."));
        trecCorpus.append(AppTest.trecDocument("5", "To be trusted is <p>a greater</p> compliment than to be loved"));
        trecCorpus.append(AppTest.trecDocument("6", "Just because everything is different doesn't mean anything has changed."));
        trecCorpus.append(AppTest.trecDocument("7", "everything everything jumped sample ugly"));
        trecCorpus.append(AppTest.trecDocument("8", "though cat moon <p>a cat</p> cat cat"));
        trecCorpus.append(AppTest.trecDocument("9", "document document document document"));
        trecCorpus.append(AppTest.trecDocument("10", "program fits"));
        trecCorpusFile = File.createTempFile("galago", ".trectext");
        Utility.copyStringToFile(trecCorpus.toString(), trecCorpusFile);

        // now, try to build an index from that
        indexFile = Utility.createTemporary();
        indexFile.delete();
        App.main(new String[]{"build-fast", "--stemming=false", indexFile.getAbsolutePath(),
                    trecCorpusFile.getAbsolutePath()});

        AppTest.verifyIndexStructures(indexFile);

    }

    public void testXCount() throws Exception {
        Parameters p = new Parameters();
        p.set("index", indexFile.getAbsolutePath());
        StructuredRetrieval retrieval = (StructuredRetrieval) Retrieval.instance(p);

        Node simpleTermExtents = StructuredQuery.parse("#extents:cat:part=postings()");
        long xc = retrieval.xCount(simpleTermExtents.toString());
        //System.err.println(xc);
        assert(xc == 5);


        Node simpleTermCounts = StructuredQuery.parse("#counts:a:part=postings()");
        xc = retrieval.xCount(simpleTermCounts.toString());
        //System.err.println(xc);
        assert(xc == 5);

        Node orderedWindow = StructuredQuery.parse("#od:2(#extents:cat:part=postings() #extents:cat:part=postings())");
        xc = retrieval.xCount(orderedWindow.toString());
        //System.err.println(xc);
        assert(xc == 2);

        Node extent = StructuredQuery.parse("#inside(#extents:a:part=postings() #extents:p:part=extents())");
        xc = retrieval.xCount(extent.toString());
        //System.err.println(xc);
        assert(xc == 3);

        retrieval.close();
    }

    @Override
    public void tearDown() throws Exception {
        if (trecCorpusFile != null) {
            trecCorpusFile.delete();
        }
        if (indexFile != null) {
            Utility.deleteDirectory(indexFile);
        }
    }
}
