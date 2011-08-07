// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.corpus;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.galagosearch.core.index.GenericIndexReader;
import org.galagosearch.core.index.KeyIterator;
import org.galagosearch.core.index.KeyToListIterator;
import org.galagosearch.core.index.KeyValueReader;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.DataIterator;
import org.galagosearch.tupleflow.DataStream;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.VByteInput;

/**
 *
 * @author trevor
 */
public class DocumentIndexReader extends KeyValueReader implements DocumentReader {

  public DocumentIndexReader(String fileName) throws FileNotFoundException, IOException {
    super(fileName);
  }

  public DocumentIndexReader(GenericIndexReader reader) {
    super(reader);
  }

  public Iterator getIterator() throws IOException {
    return new Iterator(reader);
  }

  public Document getDocument(String key) throws IOException {
    Iterator i = new Iterator(reader);
    byte[] k = Utility.fromString(key);
    if (i.findKey(k)) {
	return i.getDocument();
    } else {
	return null;
    }
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("content", new NodeType(Iterator.class));
    return types;
  }

  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("content")) {
      return new ValueIterator(new Iterator(reader));
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public class Iterator extends KeyValueReader.Iterator implements DocumentReader.DocumentIterator {

    Iterator(GenericIndexReader reader) throws IOException {
      super(reader);
    }

    public Document getDocument() throws IOException {
      String key = Utility.toString(iterator.getKey());
      DataStream stream = iterator.getValueStream();
      return decodeDocument(key, stream);
    }

    Document decodeDocument(String key, DataStream stream) throws IOException {
      VByteInput input = new VByteInput(stream);
      Document document = new Document();

      // The first string is the document text, followed by
      // key/value metadata pairs.
      document.identifier = key;
      document.text = input.readString();
      document.metadata = new HashMap<String, String>();
      document.writeTerms = input.readBoolean();
      if (document.writeTerms) {
        int count = input.readInt();
        document.terms = new ArrayList<String>();
        for (int i = 0; i < count; i++) {
          document.terms.add(input.readString());
        }
      }

      while (!stream.isDone()) {
        String mapKey = input.readString();
        String mapValue = input.readString();
        document.metadata.put(mapKey, mapValue);
      }

      return document;
    }

    public String getValueString() {
      try {
        return getDocument().toString();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    public ValueIterator getValueIterator() throws IOException {
      return new ValueIterator(this);
    }
  }

  public class ValueIterator extends KeyToListIterator implements DataIterator<Document> {

    public ValueIterator(KeyIterator ki) {
      super(ki);
    }

    public String getEntry() throws IOException {
      return ((Iterator) iterator).getDocument().toString();
    }

    public long totalEntries() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public Document getData() {
      try {
        return ((Iterator) iterator).getDocument();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }
}
