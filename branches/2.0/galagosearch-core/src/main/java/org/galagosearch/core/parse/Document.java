// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Document implements Serializable {
    public Document() {
        this.metadata = new HashMap<String, String>();
        writeTerms = false;
    }

    public Document(String identifier, String text) {
        this();
        this.identifier = identifier;
        this.text = text;
    }

    public String identifier;
    public Map<String, String> metadata;
    public String text;

    public List<String> terms;
    public List<Tag> tags;
    
    public int fileId;
    public int totalFileCount;
    public boolean writeTerms;

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Identifier: ").append(identifier).append("\n");
      if (metadata != null) {
	  sb.append("Metadata: \n");
	  for (Map.Entry<String, String> entry : metadata.entrySet()) {
	      sb.append("<");
	      sb.append(entry.getKey()).append(",").append(entry.getValue());
	      sb.append("> ");
	  }
      }

      if (terms != null) { 
	  sb.append("Term vector: \n");
	  for (String s : terms) {
	      sb.append(s).append("\n");
	  }
      }
      sb.append("\n");
      if (text != null) { 
	  sb.append("Text :").append(text);
      }
      return sb.toString();
    }

}
