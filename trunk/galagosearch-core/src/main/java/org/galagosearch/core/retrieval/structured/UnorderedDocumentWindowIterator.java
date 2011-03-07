/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import org.galagosearch.tupleflow.Parameters;

/**
 * Implementation of the #and operator. This means that as long as all internal extents have hits on the same document, 
 * that document is a candidate. The extents are loaded in document-occurrence order (earliest is first). Note that this
 * version admits overlapping (and repeating) extents for complex extent iterator children.
 * 
 * Functionally this iterator matches documents like an UnorderedWindowIterator, but the window size is always |D|.
 *
 *
 * @author irmarc
 */
public class UnorderedDocumentWindowIterator extends ExtentConjunctionIterator {

  private class ExtentComparator implements Comparator<Extent> {

    public int compare(Extent a, Extent b) {
      return (a.begin - b.begin);
    }
  }
  private ExtentComparator c;

  public UnorderedDocumentWindowIterator(Parameters parameters, ExtentIterator[] extentIterators) throws IOException {
    super(extentIterators);
    c = new ExtentComparator();
    findDocument();
  }

  /**
   * Loads all extents for a document that is matched by all internal iterators. The extents are then loaded and sorted to
   * ensure that they are in document-occurrence order (earliest is first).
   */
  @Override
  public void loadExtents() {
    extents.reset();

    ArrayList<Extent> allExtents = new ArrayList<Extent>();
    for (ExtentIterator ei : extentIterators) {
      Extent[] e = ei.extents().getBuffer();
      allExtents.addAll(Arrays.asList(e));
    }
    Collections.sort(allExtents, c);

    for (Extent e : allExtents) {
      extents.add(e);
    }
  }
}
