/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.index;

import java.io.IOException;
import org.galagosearch.core.types.NumberedDocumentData;

/**
 *
 * @author marc
 */
public abstract class NumberedDocumentDataIterator extends KeyValueReader.Iterator {
  // useful for merging indexes (and ignored by all other classes)

  public int indexId = 0;

  public NumberedDocumentDataIterator(GenericIndexReader reader) throws IOException {
    super(reader);
  }

  // Allows data to be retrieved
  public abstract NumberedDocumentData getDocumentData() throws IOException;

  public abstract boolean moveToKey(int key) throws IOException;
}
