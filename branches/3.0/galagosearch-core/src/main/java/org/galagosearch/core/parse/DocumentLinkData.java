// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.util.ArrayList;
import org.galagosearch.core.types.ExtractedLink;

/**
 *
 * @author trevor
 */
public class DocumentLinkData {
    public String identifier;
    public String url;
    public int textLength;

    public ArrayList<ExtractedLink> links;
    
    public DocumentLinkData() {
        links = new ArrayList<ExtractedLink>();
        identifier = "";
        url = "";
        textLength = 0;
    }
}
