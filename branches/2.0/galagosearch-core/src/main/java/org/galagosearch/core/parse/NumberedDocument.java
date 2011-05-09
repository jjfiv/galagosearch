// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;

public class NumberedDocument extends Document {
    public NumberedDocument() {
    	super();
    }

    public NumberedDocument(String identifier, String text) {
    	super(identifier, text);
    }

    public NumberedDocument(Document d) {
    	super(d.identifier, d.text);
    	super.metadata = d.metadata;
    	super.tags = d.tags;
    	super.terms = d.terms;
      super.fileId = d.fileId;
      super.totalFileCount = d.totalFileCount;
    }
    
    public int number;
}
