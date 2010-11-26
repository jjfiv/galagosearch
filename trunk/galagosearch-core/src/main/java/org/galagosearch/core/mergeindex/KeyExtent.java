package org.galagosearch.core.mergeindex;

import org.galagosearch.core.retrieval.structured.Extent;

public class KeyExtent {
  String key;
  Extent extent;
  int document;
  
  public KeyExtent(String key, int document, Extent e) {
    this.key = key;
    this.document = document;
    this.extent = e;
  }
  
}
