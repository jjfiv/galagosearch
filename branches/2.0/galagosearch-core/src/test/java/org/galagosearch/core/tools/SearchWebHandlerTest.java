/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import junit.framework.TestCase;

/**
 *
 * @author trevor
 */
public class SearchWebHandlerTest extends TestCase {
    
    public SearchWebHandlerTest(String testName) {
        super(testName);
    }

    public void testImage() throws IOException {
        SearchWebHandler handler = new SearchWebHandler(null);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        handler.retrieveImage(stream);
    }
}
