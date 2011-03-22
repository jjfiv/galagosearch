/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.structured;

/**
 *
 * @author marc
 */
public interface IndicatorIterator extends StructuredIterator {
  public int getIndicatorStatus();
  public boolean getStatus();
}
