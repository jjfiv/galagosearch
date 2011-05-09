// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import org.galagosearch.core.retrieval.structured.*;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.tupleflow.Parameters;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 *
 * @author trevor
 */
public class StructuredIndex {

  DocumentLengthsReader documentLengths;
  DocumentNameReader documentNames;
  Map<String, StructuredIndexPartReader> parts;
  Parameters manifest;
  HashMap<String, String> defaultIndexOperators = new HashMap<String, String>();
  HashSet<String> knownIndexOperators = new HashSet<String>();

  public StructuredIndex(String filename) throws IOException {
    manifest = new Parameters();
    manifest.parse(filename + File.separator + "manifest");
    documentLengths = new DocumentLengthsReader(filename + File.separator + "documentLengths");
    documentNames = new DocumentNameReader(filename + File.separator + "documentNames");

    File partsDirectory = new File(filename + File.separator + "parts");
    parts = new HashMap<String, StructuredIndexPartReader>();
    for (File part : partsDirectory.listFiles()) {
      StructuredIndexPartReader reader = openIndexPart(part.getAbsolutePath());
      if (reader == null) {
        continue;
      }
      parts.put(part.getName(), reader);
    }

    initializeIndexOperators();
  }

  public static String getPartPath(String index, String part) {
    return (index + File.separator + "parts" + File.separator + part);
  }

  public static StructuredIndexPartReader openIndexPart(String path) throws IOException {
    GenericIndexReader reader = GenericIndexReader.getIndexReader(path);
    if (reader == null) {
      return null;
    }

    if (!reader.getManifest().containsKey("readerClass")) {
      throw new IOException("Tried to open an index part at " + path + ", but the "
              + "file has no readerClass specified in its manifest. "
              + "(the readerClass is the class that knows how to decode the "
              + "contents of the file)");
    }

    String className = reader.getManifest().get("readerClass", (String) null);
    Class readerClass;
    try {
      readerClass = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IOException("Class " + className + ", which was specified as the readerClass "
              + "in " + path + ", could not be found.");
    }

    if (!StructuredIndexPartReader.class.isAssignableFrom(readerClass)) {
      throw new IOException(className + " is not a StructuredIndexPartReader subclass.");
    }

    Constructor c;
    try {
      c = readerClass.getConstructor(GenericIndexReader.class);
    } catch (NoSuchMethodException ex) {
      throw new IOException(className + " has no constructor that takes a single "
              + "IndexReader argument.");
    } catch (SecurityException ex) {
      throw new IOException(className + " doesn't have a suitable constructor that "
              + "this code has access to (SecurityException)");
    }

    StructuredIndexPartReader partReader;
    try {
      partReader = (StructuredIndexPartReader) c.newInstance(reader);
    } catch (Exception ex) {
      IOException e = new IOException("Caught an exception while instantiating "
              + "a StructuredIndexPartReader: ");
      e.initCause(ex);
      throw e;
    }
    return partReader;
  }

  /**
   * Tests to see if a named index part exists.
   *
   * @param partName The name of the index part to check.
   * @return true, if this index has a part called partName, or false otherwise.
   */
  public boolean containsPart(String partName) {
    return parts.containsKey(partName);
  }

  void initializeIndexOperators() {
    for (Entry<String, StructuredIndexPartReader> entry : parts.entrySet()) {
      String partName = entry.getKey();
      StructuredIndexPartReader part = entry.getValue();

      for (String name : part.getNodeTypes().keySet()) {
        knownIndexOperators.add(name);

        if (!defaultIndexOperators.containsKey(name)) {
          defaultIndexOperators.put(name, partName);
        } else if (name.startsWith("default")) {
          if (defaultIndexOperators.get(name).startsWith("default")) {
            defaultIndexOperators.remove(name);
          } else {
            defaultIndexOperators.put(name, partName);
          }
        } else {
          defaultIndexOperators.remove(name);
        }
      }
    }
  }

  private StructuredIndexPartReader getIndexPart(Node node) throws IOException {
    String operator = node.getOperator();
    StructuredIndexPartReader part = null;

    if (node.getParameters().containsKey("part")) {
      String partName = node.getParameters().get("part");
      if (!parts.containsKey(partName)) {
        throw new IOException("The index has no part named '" + partName + "'");
      }
      part = parts.get(partName);
    } else if (knownIndexOperators.contains(operator)) {
      if (!defaultIndexOperators.containsKey(operator)) {
        throw new IOException("More than one index part supplies the operator '"
                + operator + "', but no part name was specified.");
      } else {
        String partName = defaultIndexOperators.get(operator);
        part = parts.get(partName);
      }
    }
    return part;
  }

  public StructuredIterator getIterator(Node node) throws IOException {
    StructuredIterator result = null;
    StructuredIndexPartReader part = getIndexPart(node);
    if (part != null) {
      result = part.getIterator(node);
      if (result == null) {
        result = new NullExtentIterator();
      }
    }
    return result;
  }

  public NodeType getNodeType(Node node) throws IOException {
    NodeType result = null;
    StructuredIndexPartReader part = getIndexPart(node);
    if (part != null) {
      final String operator = node.getOperator();
      final Map<String, NodeType> nodeTypes = part.getNodeTypes();
      result = nodeTypes.get(operator);
    }
    return result;
  }

  public long getCollectionLength() {
    return manifest.get("collectionLength", (long) 0);
  }

  public long getDocumentCount() {
    return manifest.get("documentCount", (long) 0);
  }

  public void close() throws IOException {
    for (StructuredIndexPartReader part : parts.values()) {
      part.close();
    }
    parts.clear();
    documentLengths.close();
  }

  public int getLength(int document) throws IOException {
    return documentLengths.getLength(document);
  }

  public String getDocumentName(int document) throws IOException {
    return documentNames.get(document);
  }

  public int getDocumentNumber(String document) throws IOException {
    return documentNames.getDocumentId(document);
  }

  public NumberedDocumentDataIterator getDocumentLengthsIterator() throws IOException {
    return documentLengths.getIterator();
  }

  public NumberedDocumentDataIterator getDocumentNamesIterator() throws IOException {
    return documentNames.getNumberOrderIterator();
  }

  public Parameters getManifest() {
    return manifest.clone();
  }

  public ExtentIndexIterator getExtentIterator(String part) throws IOException {
    IndexIterator i = parts.get(part).getIterator();
    if((i != null) && (i instanceof ExtentIndexIterator)) {
      return (ExtentIndexIterator) i;
    }
    return null;
  }

  public Set<String> getPartNames() {
    return parts.keySet();
  }

  public Map<String, NodeType> getPartNodeTypes(String partName) throws IOException {
    if (!parts.containsKey(partName)) {
      throw new IOException("The index has no part named '" + partName + "'");
    }
    return parts.get(partName).getNodeTypes();
  }
}
