/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.index;

import java.io.IOException;

/**
 *
 * @author irmarc
 */
public abstract class NameReader extends KeyValueReader {

  public NameReader(GenericIndexReader reader) {
    super(reader);
  }

  public abstract String getDocumentName(int identifier) throws IOException;
}
