package org.galagosearch.core.mergeindex.parallel;

public class MappedDocument {

  public int newDocumentNumber;
  public int indexId;
  public int oldDocumentNumber;

  public MappedDocument(int newDocumentNumber, int indexId, int oldDocumentNumber) {
    this.newDocumentNumber = newDocumentNumber;
    this.indexId = indexId;
    this.oldDocumentNumber = oldDocumentNumber;
  }

}
