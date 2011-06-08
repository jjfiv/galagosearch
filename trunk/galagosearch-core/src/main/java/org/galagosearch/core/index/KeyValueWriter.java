// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import org.galagosearch.core.index.corpus.*;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import org.galagosearch.core.index.GenericElement;
import org.galagosearch.core.index.IndexWriter;
import org.galagosearch.core.parse.Document;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.VByteOutput;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 * Almost complete class. Makes assumptions that you ultimately want to write every incoming item
 * to an output file, so it handles as much boilerplate as possible. A canonical use case is the
 * DocumentContentWriter, which is used to write the corpus in Galago 2.0.
 *
 * Only thing to really implement is the prepare method.
 *
 * @author irmarc
 */
public abstract class KeyValueWriter<T> implements Processor<T> {

  protected IndexWriter writer;
  protected Counter elementsWritten;
  protected long count;

  public KeyValueWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    this(parameters, "Documents written");
  }

  public KeyValueWriter(TupleFlowParameters parameters, String text) throws FileNotFoundException, IOException {
    writer = new IndexWriter(parameters.getXML().get("filename"));
    elementsWritten = parameters.getCounter(text);
    count = 0;
  }

  protected abstract GenericElement prepare(T item) throws IOException;

  public void close() throws IOException {
    writer.getManifest().set("keyCount", Long.toString(count));
    writer.close();
  }

  public void process(T i) throws IOException {
    GenericElement e = prepare(i);
    if (e != null) {
      writer.add(e);
      count++;
      if (elementsWritten != null) {
        elementsWritten.increment();
      }
    }
  }
}
