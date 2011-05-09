// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.pagerank.iter;

import java.util.LinkedList;

import org.galagosearch.core.types.NumberedLink;
import org.galagosearch.core.types.PREntry;


/*
 * container for a PREntry and set of NumberedLinks
 * 
 * @schiu
 */
public class PRDoc extends LinkedList<NumberedLink>{

  public PREntry entry;

  public PRDoc(PREntry inEntry) {
    super();
    entry = inEntry;
  }

}
