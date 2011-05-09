// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is a logical encapsulation of a single retrieval unit.
 * The tags field is to hold XML/HTML-style markup tags.
 *
 * @author trevor
 */
public class Document implements Serializable {
    public Document() {
        this.metadata = new HashMap<String, String>();
    }

    public Document(String identifier, String text) {
        this.identifier = identifier;
        this.text = text;
        this.metadata = new HashMap<String, String>();
    }

    public String identifier;
    public Map<String, String> metadata;
    public String text;

    public List<String> terms;
    public List<Tag> tags;
    
    public int fileId;
    public int totalFileCount;

}
