// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import junit.framework.TestCase;
import org.galagosearch.core.retrieval.structured.StructuredRetrieval;
import org.galagosearch.core.index.DocumentLengthsWriter;
import org.galagosearch.core.index.DocumentNameWriter;
import org.galagosearch.core.index.ExtentIndexWriter;
import org.galagosearch.core.index.PositionIndexWriter;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.FakeParameters;
import org.galagosearch.tupleflow.TupleFlowParameters;

/**
 *
 * @author trevor
 */
public class StructuredRetrievalTest extends TestCase {
    File tempPath;

    public StructuredRetrievalTest(String testName) {
        super(testName);
    }

    public static File makeIndex() throws FileNotFoundException, IOException {
        // make a spot for the index
        File tempPath = File.createTempFile("galago-test-index", null);
        tempPath.delete();
        tempPath.mkdir();
        
        String partsPath = tempPath.toString() + File.separator + "parts";
        new File(partsPath).mkdir();
        
        // put in a generic manifest
        new Parameters().write(tempPath + File.separator + "manifest");

        // build an empty extent index
        Parameters extp = new Parameters();
        extp.add("filename", partsPath + File.separator + "extents");
        TupleFlowParameters extParameters = new FakeParameters(extp);

        ExtentIndexWriter ewriter = new ExtentIndexWriter(extParameters);
        ewriter.processExtentName(Utility.makeBytes("title"));
        ewriter.processNumber(1);
        ewriter.processBegin(1);
        ewriter.processTuple(3);
        ewriter.close();

        // write positions!
        Parameters pp = new Parameters();
        pp.add("filename", partsPath + File.separator + "terms");
        TupleFlowParameters posParameters = new FakeParameters(pp);

        PositionIndexWriter pwriter = new PositionIndexWriter(posParameters);

        pwriter.processWord(Utility.makeBytes("a"));
        pwriter.processDocument(1);
        pwriter.processPosition(1);
        pwriter.processPosition(2);
        pwriter.processPosition(3);

        pwriter.processDocument(3);
        pwriter.processPosition(1);

        pwriter.processDocument(5);
        pwriter.processPosition(1);

        pwriter.processWord(Utility.makeBytes("b"));
        pwriter.processDocument(1);
        pwriter.processPosition(2);
        pwriter.processPosition(4);

        pwriter.processDocument(2);
        pwriter.processPosition(1);

        pwriter.processDocument(3);
        pwriter.processPosition(4);

        pwriter.processDocument(18);
        pwriter.processPosition(9);
        pwriter.close();

        // add some document names
        Parameters dnp = new Parameters();
        dnp.add("filename", tempPath + File.separator + "documentNames");

        DocumentNameWriter dnWriter = new DocumentNameWriter(new FakeParameters(dnp));
        for (int i = 0; i < 20; i++) {
            dnWriter.process(new NumberedDocumentData("DOC" + i, "", i, 100));
        }
        dnWriter.close();

        Parameters lp = new Parameters();
        lp.add("filename", tempPath + File.separator + "documentLengths");
        DocumentLengthsWriter lWriter = new DocumentLengthsWriter(new FakeParameters(lp));

        for (int i = 0; i < 20; i++) {
            lWriter.process(new NumberedDocumentData("DOC" + i, "", i, 100));
        }
        lWriter.close();

        // main manifest file
        Parameters mainParameters = new Parameters();
        mainParameters.add("collectionLength", "10000");

        mainParameters.write(tempPath + File.separator + "manifest");
        return tempPath;
    }

    @Override
    public void setUp() throws IOException {
        this.tempPath = makeIndex();
    }

    @Override
    public void tearDown() throws IOException {
        Utility.deleteDirectory(tempPath);
    }

    public void testDocuments() throws FileNotFoundException, IOException {
        StructuredRetrieval retrieval = new StructuredRetrieval(tempPath.toString(), new Parameters());
        String name;

        name = retrieval.getDocumentName(1);
        assertEquals("DOC1", name);
        name = retrieval.getDocumentName(2);
        assertEquals("DOC2", name);
        name = retrieval.getDocumentName(3);
        assertEquals("DOC3", name);
        name = retrieval.getDocumentName(4);
        assertEquals("DOC4", name);
        name = retrieval.getDocumentName(5);
        assertEquals("DOC5", name);
    }

    public void testSimple() throws FileNotFoundException, IOException, Exception {
        StructuredRetrieval retrieval = new StructuredRetrieval(tempPath.toString(), new Parameters());

        Node aTerm = new Node("counts", "a");
        ArrayList<Node> aChild = new ArrayList<Node>();
        aChild.add(aTerm);
        Parameters a = new Parameters();
        a.add("default", "dirichlet");
        a.add("mu", "1500");
        Node aFeature = new Node("feature", a, aChild, 0);

        Node bTerm = new Node("counts", "b");
        ArrayList<Node> bChild = new ArrayList<Node>();
        Parameters b = new Parameters();
        b.add("default", "dirichlet");
        b.add("mu", "1500");
        bChild.add(bTerm);
        Node bFeature = new Node("feature", b, bChild, 0);

        ArrayList<Node> children = new ArrayList<Node>();
        children.add(aFeature);
        children.add(bFeature);
        Node root = new Node("combine", children);

        ScoredDocument[] result = retrieval.runQuery(root, 5);

        assertEquals(result.length, 5);

        HashMap<Integer, Double> realScores = new HashMap<Integer, Double>();

        realScores.put(1, -12.422161102294922);
        realScores.put(3, -13.636285781860352);
        realScores.put(5, -14.483583450317383);
        realScores.put(18, -14.483583450317383);
        realScores.put(2, -14.483583450317383);

        // make sure the results are sorted
        double lastScore = Double.MAX_VALUE;

        for (int i = 0; i < result.length; i++) {
            double score = result[i].score;
            double expected = realScores.get(result[i].document);

            assertTrue(lastScore >= result[i].score);
            assertEquals(expected, score, 0.0001);

            lastScore = score;
        }
    }
}
