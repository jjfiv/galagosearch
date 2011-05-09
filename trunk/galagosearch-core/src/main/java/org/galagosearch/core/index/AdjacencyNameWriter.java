/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.galagosearch.tupleflow.TupleFlowParameters;

/**
 *
 * @author irmarc
 */
public class AdjacencyNameWriter extends KeyValueWriter<GenericElement> {
  public AdjacencyNameWriter(TupleFlowParameters tfp)
          throws FileNotFoundException, IOException {
    super(tfp);
    writer.getManifest().set("writerClass", AdjacencyNameWriter.class.getName());
    writer.getManifest().set("readerClass", AdjacencyNameReader.class.getName());
  }

  protected GenericElement prepare(GenericElement item) throws IOException {
    return item;
  }
}
