// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.tools;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import junit.framework.TestCase;
import org.galagosearch.tupleflow.execution.ErrorStore;
import org.galagosearch.tupleflow.Utility;
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

    public String trecDocument(String docno, String text) {
        return "<DOC>\n<DOCNO>" + docno + "</DOCNO>\n" +
               "<TEXT>\n" + text + "</TEXT>\n</DOC>\n";
    }

    public void testSimplePipeline() throws Exception {
        File relsFile = null;
        File queryFile = null;
        File scoresFile = null;
        File trecCorpusFile = null;
        File corpusFile = null;
        File indexFile = null;

        try {
            // create a simple doc file, trec format:
            String trecCorpus = trecDocument("55", "This is a sample document") +
                                trecDocument("59", "sample document two");
            trecCorpusFile = File.createTempFile("galago", ".trectext");
            Utility.copyStringToFile(trecCorpus, trecCorpusFile);

            // now, attempt to make a corpus file from that.
            corpusFile = File.createTempFile("galago", ".corpus");
            App.main(new String[] { "make-corpus", corpusFile.getAbsolutePath(),
                                    trecCorpusFile.getAbsolutePath() } );

            // make sure the corpus file exists
            assertTrue(corpusFile.exists());

            // now, try to build an index from that
            indexFile = Utility.createTemporary();
            indexFile.delete();
            App.main(new String[] { "build", indexFile.getAbsolutePath(),
                                    corpusFile.getAbsolutePath() });

            assertTrue(indexFile.exists());

            // try to batch search that index with a no-match string
            String queries =
                "<parameters>\n" +
                "<query><number>5</number><text>nothing</text></query>\n" +
                "<query><number>9</number><text>sample</text></query>\n" +
                "<query><number>10</number><text>nothing sample</text></query>\n" +
                "<query><number>14</number><text>#combine(1(this is) sample)</text></query>\n" +
                "</parameters>\n";
            queryFile = Utility.createTemporary();
            Utility.copyStringToFile(queries, queryFile);

            // Smoke test with batch search
            ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
            PrintStream printStream = new PrintStream(byteArrayStream);

            new App(printStream).run(new String[] { "batch-search",
                                                    "--index=" + indexFile.getAbsolutePath(),
                                                    queryFile.getAbsolutePath() } );

            // Now, verify that some stuff exists
            String output = byteArrayStream.toString();
            String expectedScores = "9 Q0 59 1 -1.38562930 galago\n" +
                                    "9 Q0 55 2 -1.38695908 galago\n" +
                                    "10 Q0 59 1 -4.16021585 galago\n" +
                                    "10 Q0 55 2 -4.16287565 galago\n" +
                                    "14 Q0 59 1 -6.93480253 galago\n" +
                                    "14 Q0 55 2 -6.93879223 galago\n";
            assertEquals(expectedScores, output);

            // Verify dump-keys works
            byteArrayStream = new ByteArrayOutputStream();
            printStream = new PrintStream(byteArrayStream);

            new App(printStream).run(new String[] { "dump-keys", corpusFile.getAbsolutePath() });
            output = byteArrayStream.toString();
            assertEquals("55\n59\n", output);

            // Verify doc works
            byteArrayStream = new ByteArrayOutputStream();
            printStream = new PrintStream(byteArrayStream);

            new App(printStream).run(new String[] { "doc", corpusFile.getAbsolutePath(), "55" });
            output = byteArrayStream.toString();
            assertEquals("<TEXT>\nThis is a sample document</TEXT>\n\n", output);

            // Verify dump-index works
            byteArrayStream = new ByteArrayOutputStream();
            printStream = new PrintStream(byteArrayStream);

            String postingsName = Utility.join(new String[] { indexFile.getAbsolutePath(),
                                                              "parts", "stemmedPostings" },
                                               File.separator);
            new App(printStream).run(new String[] { "dump-index", postingsName });
            output = byteArrayStream.toString();
            assertEquals("a,0,2\n" +
                         "document,0,4\n" +
                         "document,1,1\n" +
                         "is,0,1\n" +
                         "sampl,0,3\n" +
                         "sampl,1,0\n" +
                         "this,0,0\n" +
                         "two,1,2\n", output);

            // Verify eval works
            byteArrayStream = new ByteArrayOutputStream();
            printStream = new PrintStream(byteArrayStream);

            scoresFile = Utility.createTemporary();
            Utility.copyStringToFile(expectedScores, scoresFile);
            relsFile = Utility.createTemporary();
            Utility.copyStringToFile("9 Q0 55 1\n", relsFile);

            // for now this is just a smoke test.
            new App(printStream).run(new String[] { "eval", scoresFile.getAbsolutePath(), relsFile.getAbsolutePath() });
        } finally {
            if (relsFile != null) relsFile.delete();
            if (queryFile != null) queryFile.delete();
            if (scoresFile != null) scoresFile.delete();
            if (trecCorpusFile != null) trecCorpusFile.delete();
            if (corpusFile != null) corpusFile.delete();
            if (indexFile != null) Utility.deleteDirectory(indexFile);
        }
    }

    public void testUsage() throws IOException {
        Job job = App.getDocumentConverter("in", new String[] {"/"});
        ErrorStore store = new ErrorStore();

        Verification.verify(job, store);
        assertEquals("", store.toString());
    }
}
