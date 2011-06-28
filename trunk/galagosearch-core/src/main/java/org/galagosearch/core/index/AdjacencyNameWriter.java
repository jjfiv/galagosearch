/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.tupleflow.execution.ErrorHandler;

/**
 *
 * @author irmarc
 */
@InputClass(className = "org.galagosearch.core.types.KeyValuePair")
public class AdjacencyNameWriter extends KeyValueWriter<KeyValuePair> {
  public AdjacencyNameWriter(TupleFlowParameters tfp)
          throws FileNotFoundException, IOException {
    super(tfp);
    writer.getManifest().set("writerClass", AdjacencyNameWriter.class.getName());
    writer.getManifest().set("readerClass", AdjacencyNameReader.class.getName());
  }

  protected GenericElement prepare(KeyValuePair item) throws IOException {
    GenericElement ge = new GenericElement(item.key, item.value);
    return ge;
  }

  public static void verify(TupleFlowParameters p, ErrorHandler h) {
    KeyValueWriter.verify(p, h);
  }
}
