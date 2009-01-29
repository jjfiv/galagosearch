/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.parse;

import org.galagosearch.core.parse.IndexReaderSplitParser;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.parse.DocumentIndexWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import junit.framework.TestCase;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.core.index.IndexWriter;
import org.galagosearch.core.index.GenericElement;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.tupleflow.FakeParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.core.types.DocumentSplit;

/**
 *
 * @author trevor
 */
public class IndexReaderSplitParserTest extends TestCase {
    Document document;
    String temporaryName;
    
    public IndexReaderSplitParserTest(String testName) {
        super(testName);
    }
    
    @Override
    public void tearDown() {
        if (temporaryName.length() != 0)
            new File(temporaryName).delete();
    }

    public void buildIndex() throws FileNotFoundException, IOException {
        File temporary = Utility.createTemporary();

        // Build an encoded document:
        document = new Document();
        document.identifier = "doc-identifier";
        document.text = "This is the text part.";
        document.metadata.put("Key", "Value");
        document.metadata.put("Something", "Else");
        
        Parameters parameters = new Parameters();
        parameters.add("filename", temporary.getAbsolutePath());
        DocumentIndexWriter writer = new DocumentIndexWriter(new FakeParameters(parameters));
        writer.process(document);
        writer.close();
    
        temporaryName = temporary.getAbsolutePath();
        assertTrue(IndexReader.isIndexFile(temporaryName));
    }
    
    /**
     * Test of nextDocument method, of class IndexReaderSplitParser.
     */
    public void testNextDocument() throws Exception {
        buildIndex();
        
        DocumentSplit split = new DocumentSplit();
        split.fileName = temporaryName;
        split.fileType = "corpus";
        split.startKey = new byte[0];
        split.endKey = new byte[0];
        
        // Open up the file:
        IndexReaderSplitParser parser = new IndexReaderSplitParser(split);
        
        // Check the document:
        Document actual = parser.nextDocument();
        assertNotNull(actual);
        assertEquals(document.identifier, actual.identifier);
        assertEquals(document.text, actual.text);
        assertEquals(2, actual.metadata.size());
        assertNotNull(document.metadata.get("Key"));
        assertNotNull(document.metadata.get("Something"));
        assertEquals("Value", document.metadata.get("Key"));
        assertEquals("Else", document.metadata.get("Something"));
        
        // Make sure there aren't any left:
        assertNull(parser.nextDocument());
    }
    
    public void testStartKey() throws FileNotFoundException, IOException {
        buildIndex();
              
        DocumentSplit split = new DocumentSplit();
        split.fileName = temporaryName;
        split.fileType = "corpus";
        split.startKey = new byte[] { (byte) 'z' };
        split.endKey = new byte[0];
        
        // Open up the file:
        IndexReaderSplitParser parser = new IndexReaderSplitParser(split);
        assertNull(parser.nextDocument());
    }

    public void testEndKey() throws FileNotFoundException, IOException {
        buildIndex();
              
        DocumentSplit split = new DocumentSplit();
        split.fileName = temporaryName;
        split.fileType = "corpus";
        split.startKey = new byte[0];
        split.endKey = new byte[] { (byte) 'a' };
        
        // Open up the file:
        IndexReaderSplitParser parser = new IndexReaderSplitParser(split);
        assertNull(parser.nextDocument());
    }
}
