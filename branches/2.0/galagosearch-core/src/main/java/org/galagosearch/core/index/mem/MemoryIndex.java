// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.mem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Map;
import org.galagosearch.core.index.StructuredIndexPartReader;

import org.galagosearch.core.retrieval.structured.ExtentIndexIterator;
import org.galagosearch.core.parse.NumberedDocument;
import org.galagosearch.core.parse.Porter2Stemmer;
import org.galagosearch.core.parse.Tag;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.IndexIterator;
import org.galagosearch.core.retrieval.structured.NullExtentIterator;
import org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator;
import org.galagosearch.core.retrieval.structured.StructuredIterator;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.NullProcessor;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;

/*
 * Memory Index
 * 
 * Assumptions; documents are added sequentially
 *
 * author: sjh, schiu
 * 
 */
@Verified
@InputClass(className = "org.galagosearch.core.parse.NumberedDocument")
public class MemoryIndex implements Processor<NumberedDocument> {

  private boolean stemming;
  private int lastDocId;
  private MemoryManifest manifest;
  private MemoryDocumentLengths documentLengths;
  private MemoryDocumentNames documentNames;
  private MemoryExtents extents;
  private MemoryPostings postings;
  private MemoryPostings stemmedPostings;
  private HashMap<String,StructuredIndexPartReader> parts;

  public MemoryIndex(TupleFlowParameters parameters) {
    this(parameters.getXML());
  }

  public MemoryIndex(Parameters parameters) {
    stemming = (boolean) parameters.get("stemming", true);
    int documentNumberOffset = (int) parameters.get("firstDocumentId", 0);
    lastDocId = documentNumberOffset - 1;

    manifest = new MemoryManifest();
    manifest.setOffset(documentNumberOffset);
    documentLengths = new MemoryDocumentLengths(documentNumberOffset);
    documentNames = new MemoryDocumentNames(documentNumberOffset);
    extents = new MemoryExtents();
    postings = new MemoryPostings();
    parts = new HashMap();
    parts.put("postings",postings);
    parts.put("extents",extents);

    if (stemming) {
      stemmedPostings = new MemoryPostings();
      parts.put("stemmedPostings",stemmedPostings);
    }

  }

  public void process(NumberedDocument doc) throws IOException {
    assert (doc.number == lastDocId + 1) : "Recieved document number "
            + doc.number + " expected " + (lastDocId + 1);
    lastDocId = doc.number;

    try {
      manifest.addDocument(doc.terms.size());
      documentLengths.addDocument(doc.number, doc.terms.size());
      documentNames.addDocument(doc.number, doc.identifier);
      for (Tag tag : doc.tags) {
        // I assume that tags are in order (+begin)
        extents.addDocumentExtent(tag.name, doc.number, tag);
      }
      for (int i = 0; i < doc.terms.size(); i++) {
        if (doc.terms.get(i) != null) {
          postings.addPosting(Utility.fromString(doc.terms.get(i)), doc.number, i);
        }
      }

      if (stemming) {
        Porter2Stemmer stemmer = new Porter2Stemmer();
        stemmer.setProcessor(new NullProcessor());
        stemmer.process(doc);
        for (int i = 0; i < doc.terms.size(); i++) {
          if (doc.terms.get(i) != null) {
            stemmedPostings.addPosting(Utility.fromString(doc.terms.get(i)),
                    doc.number, i);
          }
        }

      }

    } catch (IndexOutOfBoundsException e) {
      // logger.log(Level.INFO, "Problem indexing document: " +
      // e.toString());
      throw new RuntimeException(
              "Memory Indexer failed to add document: " + doc.identifier);
    } catch (IncompatibleProcessorException e) {
      // logger.log(Level.INFO, "Problem stemming document: " +
      // e.toString());
    }
  }

  public void close() throws IOException {
    /*
     * flush to disk
     *
     * File temp = Utility.createGalagoTempDir();
     * System.err.println(temp.getAbsoluteFile()); (new
     * FlushToDisk()).flushMemoryIndex(this, temp.getAbsolutePath());
     */

    // try to free some memory up
    manifest = null;
    documentLengths = null;
    documentNames = null;
    extents = null;
    postings = null;
    stemmedPostings = null;
    parts = null;
  }

  /*
   * Index functions
   *
   * Needed to support index merging / index dumping
   */
  public long getCollectionLength() {
    return manifest.getCollectionLength();
  }

  public long getDocumentCount() {
    return manifest.getDocumentCount();
  }

  public int getDocumentNumberOffset() {
    return manifest.getOffset();
  }

  public Parameters getManifest() {
    return manifest.makeParameters();
  }

  public String getDocumentName(int document) throws IOException {
    return documentNames.getDocumentName(document);
  }

  public int getDocumentNumber(String document) throws IOException {
    return documentNames.getDocumentId(document);
  }

  public NumberedDocumentDataIterator getDocumentNamesIterator() {
    return documentNames.getIterator();
  }

  public NumberedDocumentDataIterator getDocumentLengthsIterator() {
    return documentLengths.getIterator();
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

    // sjh - we don't have any defaults for memory index operators yet.
    //} else if (knownIndexOperators.contains(operator)) {
    //  if (!defaultIndexOperators.containsKey(operator)) {
    //    throw new IOException("More than one index part supplies the operator '"
    //            + operator + "', but no part name was specified.");
    //  } else {
    //    String partName = defaultIndexOperators.get(operator);
    //    part = parts.get(partName);
    //  }
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

  public ExtentIndexIterator getExtentIterator(String part) throws IOException {
    IndexIterator i = parts.get(part).getIterator();
    if (i instanceof ExtentIndexIterator) {
      return (ExtentIndexIterator) i;
    }
    throw new RuntimeException("part " + part + " does not offer an Extent Index Iterator");
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
