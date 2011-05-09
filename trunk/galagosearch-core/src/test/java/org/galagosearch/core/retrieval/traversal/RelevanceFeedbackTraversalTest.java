package org.galagosearch.core.retrieval.traversal;

import java.io.File;
import junit.framework.TestCase;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.retrieval.RetrievalFactory;
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
public class RelevanceFeedbackTraversalTest extends TestCase {

    File relsFile = null;
    File queryFile = null;
    File scoresFile = null;
    File trecCorpusFile = null;
    File corpusFile = null;
    File indexFile = null;

    public RelevanceFeedbackTraversalTest(String testName) {
        super(testName);
    }

    // Build an index based on 10 short docs
    @Override
    public void setUp() throws Exception {

        // create a simple doc file, trec format:
        StringBuilder trecCorpus = new StringBuilder();
        trecCorpus.append(AppTest.trecDocument("1", "This is a sample document"));
        trecCorpus.append(AppTest.trecDocument("2", "The cat jumped over the moon"));
        trecCorpus.append(AppTest.trecDocument("3", "If the shoe fits, it's ugly"));
        trecCorpus.append(AppTest.trecDocument("4", "Though a program be but three lines long, someday it will have to be maintained."));
        trecCorpus.append(AppTest.trecDocument("5", "To be trusted is a greater compliment than to be loved"));
        trecCorpus.append(AppTest.trecDocument("6", "Just because everything is different doesn't mean anything has changed."));
        trecCorpus.append(AppTest.trecDocument("7", "everything everything jumped sample ugly"));
        trecCorpus.append(AppTest.trecDocument("8", "though cat moon cat cat cat"));
        trecCorpus.append(AppTest.trecDocument("9", "document document document document"));
        trecCorpus.append(AppTest.trecDocument("10", "program fits"));
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

    public void testRelevanceModelTraversal() throws Exception {
        // Create a retrieval object for use by the traversal
        Parameters p = new Parameters();
        p.add("retrievalGroup", "all");
        p.set("index", indexFile.getAbsolutePath());
        p.set("corpus", corpusFile.getAbsolutePath());
        StructuredRetrieval retrieval = (StructuredRetrieval) RetrievalFactory.instance(p);
        RelevanceModelTraversal traversal = new RelevanceModelTraversal(p, retrieval);

        Node parsedTree = StructuredQuery.parse("#rm:fbTerms=3:fbDocs=2( #feature:dirichlet( #extents:fits:part=postings() ) )");
        Node transformed = StructuredQuery.copy(traversal, parsedTree);
        // truth data
        StringBuilder correct = new StringBuilder();
        correct.append("#combine:1=@/0.5/:0=@/0.5/( ");
        correct.append("#combine( #feature:dirichlet( #extents:fits:part=postings() ) ) ");
        correct.append("#combine:2=@/0.041611258865248225/:1=@/0.041611258865248225/:0=@/0.12516622340425534/( ");
        correct.append("#feature:dirichlet( #extents:program:part=postings() ) ");
        correct.append("#feature:dirichlet( #extents:shoe:part=postings() ) ");
        correct.append("#feature:dirichlet( #extents:ugly:part=postings() ) ) )");
        assertEquals(correct.toString(), transformed.toString());
        retrieval.close();
    }

    public void testBM25RelevanceFeedbackTraversal() throws Exception {
        // Create a retrieval object for use by the traversal
        Parameters p = new Parameters();
        p.add("retrievalGroup", "all");
        p.set("index", indexFile.getAbsolutePath());
        p.set("corpus", corpusFile.getAbsolutePath());
        StructuredRetrieval retrieval = (StructuredRetrieval) RetrievalFactory.instance(p);
        BM25RelevanceFeedbackTraversal traversal = new BM25RelevanceFeedbackTraversal(p, retrieval);
        Node parsedTree = StructuredQuery.parse("#bm25rf:fbDocs=3:fbTerms=2( #feature:bm25( #extents:cat:part=postings() ) )");
        Node transformed = StructuredQuery.copy(traversal, parsedTree);
        //truth data
        StringBuilder correct = new StringBuilder();
        correct.append("#combine( #feature:bm25( #extents:cat:part=postings() ) ");
        correct.append("#feature:bm25rf:R=3:rt=2( #extents:moon:part=postings() ) ");
        correct.append("#feature:bm25rf:R=3:rt=1( #extents:jumped:part=postings() ) )");
        assertEquals(correct.toString(), transformed.toString());
        retrieval.close();
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
}
