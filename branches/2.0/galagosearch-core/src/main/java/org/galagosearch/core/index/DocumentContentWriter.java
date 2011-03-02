// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import org.galagosearch.core.index.corpus.*;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.galagosearch.core.parse.Document;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.VByteOutput;

/**
 * Writes document text and metadata to an index file.  The output files
 * are in '.corpus' format, which can be fed to UniversalParser as an input
 * to indexing.  The '.corpus' format is also convenient for quickly
 * finding individual documents.
 * 
 * @author trevor
 * @author irmarc
 */
@InputClass(className = "org.galagosearch.core.parse.Document")
public class DocumentContentWriter extends KeyValueWriter<Document> {

  public DocumentContentWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    super(parameters);

    // parameterize the writer
    Parameters p = writer.getManifest();
    p.add("isCompressed", "true");
    p.add("writerClass", getClass().getName());
    p.add("readerClass", DocumentIndexReader.class.getName());
  }

  protected GenericElement prepare(Document document) throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    VByteOutput output = new VByteOutput(new DataOutputStream(stream));

    Set<Map.Entry<String, String>> entries = document.metadata.entrySet();
    output.writeInt(entries.size());
    for (Map.Entry<String, String> entry : entries) {
      output.writeString(entry.getKey());
      output.writeString(entry.getValue());
    }
    output.writeString(document.text);
    return new GenericElement(document.identifier, stream.toByteArray());
  }
}
