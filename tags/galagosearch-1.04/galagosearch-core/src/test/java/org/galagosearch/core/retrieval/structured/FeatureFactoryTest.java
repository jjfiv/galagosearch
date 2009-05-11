package org.galagosearch.core.retrieval.structured;

import org.galagosearch.core.retrieval.structured.ExtentIterator;
import org.galagosearch.core.retrieval.structured.FeatureFactory;
import org.galagosearch.core.retrieval.structured.UnfilteredCombinationIterator;
import org.galagosearch.core.retrieval.structured.StructuredIterator;
import org.galagosearch.core.retrieval.structured.NullExtentIterator;
import org.galagosearch.core.retrieval.structured.OrderedWindowIterator;
import org.galagosearch.core.retrieval.structured.SynonymIterator;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.scoring.DirichletScorer;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class FeatureFactoryTest extends TestCase {
    
    public FeatureFactoryTest(String testName) {
        super(testName);
    }

    /**
     * Test of getClassName method, of class FeatureFactory.
     */
    public void testGetClassName() throws Exception {
        FeatureFactory f = new FeatureFactory(new Parameters());
        String actual = f.getClassName(new Node("syn", "fakeargument"));
        assertEquals(SynonymIterator.class.getName(), actual);
    }

    /**
     * Test of getFeatureClassName method, of class FeatureFactory.
     */
    public void testGetFeatureClassName() throws Exception {
        FeatureFactory f = new FeatureFactory(new Parameters());
        Parameters p = new Parameters();
        p.add("default", "dirichlet");
        String actual = f.getFeatureClassName(p);
        assertEquals(DirichletScorer.class.getName(), actual);
    }

    /**
     * Test of getClass method, of class FeatureFactory.
     */
    public void testGetClass() throws Exception {
        FeatureFactory f = new FeatureFactory(new Parameters());
        Class c = f.getClass(new Node("combine", ""));
        assertEquals(UnfilteredCombinationIterator.class.getName(), c.getName());
    }

    /**
     * Test of getNodeType method, of class FeatureFactory.
     */
    public void testGetNodeType() throws Exception {
        FeatureFactory f = new FeatureFactory(new Parameters());
        NodeType type = f.getNodeType(new Node("combine", ""));
        Class c = type.getIteratorClass();
        assertEquals(UnfilteredCombinationIterator.class.getName(), c.getName());
    }

    /**
     * Test of isUsableConstructor method, of class FeatureFactory.
     */
    public void testIsUsableConstructor() {
        FeatureFactory f = new FeatureFactory(new Parameters());
        Class[] types = new Class[] { Parameters.class };
        ArrayList<StructuredIterator> iterators = new ArrayList();
        assertTrue(f.isUsableConstructor(types, iterators));
        
        types = new Class[] { Parameters.class, ExtentIterator.class };
        iterators.clear();
        iterators.add(new NullExtentIterator());
        assertTrue(f.isUsableConstructor(types, iterators));
    }

    /**
     * Test of argsForConstructor method, of class FeatureFactory.
     */
    public void testArgsForConstructor() {
        FeatureFactory f = new FeatureFactory(new Parameters());
        Parameters p = new Parameters();
        Class[] types = new Class[] { Parameters.class };
        Object[] args = f.argsForConstructor(types, p, new ArrayList());
        assertEquals(1, args.length);
        assertEquals(p, args[0]);
    }

    public void testArrayArgsForConstructor() {
        FeatureFactory f = new FeatureFactory(new Parameters());
        ArrayList<StructuredIterator> iterators = new ArrayList();
        iterators.add(new NullExtentIterator());
        ExtentIterator[] fakeArray = new ExtentIterator[1];
        Class[] types = { Parameters.class, fakeArray.getClass() };
        Object[] args = f.argsForConstructor(types, new Parameters(), iterators);
        assertEquals(2, args.length);
        assertEquals(Parameters.class.getName(), args[0].getClass().getName());
        
        ExtentIterator[] array = (ExtentIterator[]) args[1];
        assertEquals(iterators.get(0), array[0]);
    }
    
    /**
     * Test of getIterator method, of class FeatureFactory.
     */
    public void testGetIterator() throws Exception {
        FeatureFactory f = new FeatureFactory(new Parameters());
        ArrayList<StructuredIterator> iterators = new ArrayList();
        iterators.add(new NullExtentIterator());
        StructuredIterator iterator = f.getIterator(new Node("od", "5"), iterators);
        assertEquals(OrderedWindowIterator.class.getName(), iterator.getClass().getName());
    }

    public void testGetClassNameConfig() throws Exception {
        String config = "" +
            "<parameters>\n" +
            "    <operators>\n" +
            "        <operator>\n" +
            "            <class>b</class>\n" +
            "            <name>a</name>\n" +
            "        </operator>\n" +
            "    </operators>\n" +
            "</parameters>";
        Parameters p = new Parameters(config.getBytes("UTF-8"));
        FeatureFactory f = new FeatureFactory(p);

        assertEquals("b", f.getClassName(new Node("a", new ArrayList())));
    }

    public void testGetTraversalNames() throws Exception {
        String config = "" +
            "<parameters>\n" +
            "    <traversals>\n" +
            "        <traversal>\n" +
            "            <class>b</class>\n" +
            "            <order>after</order>\n" +
            "        </traversal>\n" +
            "        <traversal>\n" +
            "            <class>a</class>\n" +
            "            <order>before</order>\n" +
            "        </traversal>\n" +
            "        <traversal>\n" +
            "            <class>c</class>\n" +
            "            <order>before</order>\n" +
            "        </traversal>\n" +
            "    </traversals>\n" +
            "</parameters>";
        Parameters p = new Parameters(config.getBytes("UTF-8"));
        FeatureFactory f = new FeatureFactory(p);
        List<String> traversalNames = f.getTraversalNames();

        assertEquals("a", traversalNames.get(0));
        assertEquals("c", traversalNames.get(1));
        assertEquals("b", traversalNames.get(traversalNames.size()-1));
    }
}
