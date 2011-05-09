package org.galagosearch.core.retrieval.structured;

import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.tupleflow.Parameters;

/**
 * 12/12/2010 [irmarc]: Due to the refactor for parallel retrieval stacks, all FeatureFactory 
 * references currently point to the DocumentOrderedFeatureFactory.
 * ImpactOrderedFeatureFactory testing, which isn't much, will be added later.
 *
 * @author trevor, irmarc
 */
public class FeatureFactoryTest extends TestCase {
    
    public FeatureFactoryTest(String testName) {
        super(testName);
    }

    /**
     * Test of getClassName method, of class FeatureFactory.
     */
    public void testGetClassName() throws Exception {
        DocumentOrderedFeatureFactory f = new DocumentOrderedFeatureFactory(new Parameters());
        String actual = f.getClassName(new Node("syn", "fakeargument"));
        assertEquals(SynonymIterator.class.getName(), actual);
    }

    /**
     * Test of getFeatureClassName method, of class FeatureFactory.
     */
    public void testGetFeatureClassName() throws Exception {
        DocumentOrderedFeatureFactory f = new DocumentOrderedFeatureFactory(new Parameters());
        Parameters p = new Parameters();
        p.add("default", "dirichlet");
        String actual = f.getFeatureClassName(p);
        assertEquals(DirichletScoringIterator.class.getName(), actual);
    }

    /**
     * Test of getClass method, of class FeatureFactory.
     */
    public void testGetClass() throws Exception {
        DocumentOrderedFeatureFactory f = new DocumentOrderedFeatureFactory(new Parameters());
        Class c = f.getClass(new Node("combine", ""));
        assertEquals(UnfilteredCombinationIterator.class.getName(), c.getName());
    }

    /**
     * Test of getNodeType method, of class FeatureFactory.
     */
    public void testGetNodeType() throws Exception {
        DocumentOrderedFeatureFactory f = new DocumentOrderedFeatureFactory(new Parameters());
        NodeType type = f.getNodeType(new Node("combine", ""));
        Class c = type.getIteratorClass();
        assertEquals(UnfilteredCombinationIterator.class.getName(), c.getName());
    }

    /**
     * Test of isUsableConstructor method, of class FeatureFactory.
     */
    public void testIsUsableConstructor() {
        DocumentOrderedFeatureFactory f = new DocumentOrderedFeatureFactory(new Parameters());
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
        DocumentOrderedFeatureFactory f = new DocumentOrderedFeatureFactory(new Parameters());
        Parameters p = new Parameters();
        Class[] types = new Class[] { Parameters.class };
        Object[] args = f.argsForConstructor(types, p, new ArrayList());
        assertEquals(1, args.length);
        assertEquals(p, args[0]);
    }

    public void testArrayArgsForConstructor() {
        DocumentOrderedFeatureFactory f = new DocumentOrderedFeatureFactory(new Parameters());
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
        DocumentOrderedFeatureFactory f = new DocumentOrderedFeatureFactory(new Parameters());
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
        DocumentOrderedFeatureFactory f = new DocumentOrderedFeatureFactory(p);

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
        DocumentOrderedFeatureFactory f = new DocumentOrderedFeatureFactory(p);
        List<String> traversalNames = f.getTraversalNames();

        assertEquals("a", traversalNames.get(0));
        assertEquals("c", traversalNames.get(1));
        assertEquals("b", traversalNames.get(traversalNames.size()-1));
    }
}
