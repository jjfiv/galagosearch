// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.parse;

public class NumberedDocument extends Document {

  public int number;

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
}
