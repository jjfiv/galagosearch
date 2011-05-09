package org.galagosearch.tupleflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import junit.framework.TestCase;
import org.galagosearch.tupleflow.types.FileName;

/**
 *
 * @author trevor
 */
public class TextWriterTest extends TestCase {
    File tempPath;

    public TextWriterTest(String testName) {
        super(testName);
    }

    @Override
    public void setUp() throws IOException {
        tempPath = Utility.createTemporary();
    }

    @Override
    public void tearDown() {
        if (tempPath != null)
            tempPath.delete();
    }

    public void testWriter() throws Exception {
        Parameters p = new Parameters();
        p.add("class", FileName.class.getName());
        p.add("filename", tempPath.getAbsolutePath());
        TextWriter writer = new TextWriter(new FakeParameters(p));

        writer.process(new FileName("hey"));
        writer.process(new FileName("you"));
        writer.close();

        BufferedReader reader = new BufferedReader(new FileReader(tempPath.getAbsolutePath()));
        String line;
        line = reader.readLine();
        assertEquals("hey", line);
        line = reader.readLine();
        assertEquals("you", line);
        line = reader.readLine();
        assertEquals(null, line);
    }
}
