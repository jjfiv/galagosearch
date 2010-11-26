package org.galagosearch.core.retrieval.structured;

import org.galagosearch.core.retrieval.structured.ExtentIterator;
import org.galagosearch.core.retrieval.structured.IndexIterator;

// Combines the extent and index iterator interfaces
// Allows for a simpler merge index function
public abstract class ExtentIndexIterator extends ExtentIterator implements IndexIterator{

  public int indexId = 0;
}
