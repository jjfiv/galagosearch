/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.tupleflow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import junit.framework.TestCase;

/**
 *
 * @author trevor
 */
public class VByteInputTest extends TestCase {
    
    public VByteInputTest(String testName) {
        super(testName);
    }

    public void testReadString() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        VByteOutput output = new VByteOutput(new DataOutputStream(stream));
        output.writeString("\u2297");
        stream.close();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(stream.toByteArray());
        VByteInput input = new VByteInput(new DataInputStream(inputStream));
        String result = input.readString();

        assertEquals("\u2297", result); 
    }

}
