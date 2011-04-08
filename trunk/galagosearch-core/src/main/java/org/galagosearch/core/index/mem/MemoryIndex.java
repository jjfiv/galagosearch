// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.mem;

import java.io.IOException;
import java.util.Set;
import java.util.Map;
import java.util.TreeSet;
import org.galagosearch.core.index.StructuredIndexPartReader;

import org.galagosearch.core.retrieval.structured.ExtentIndexIterator;
import org.galagosearch.core.parse.NumberedDocument;
import org.galagosearch.core.parse.Porter2Stemmer;
import org.galagosearch.core.parse.Tag;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.IndexIterator;
import org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator;
import org.galagosearch.core.retrieval.structured.StructuredIterator;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.NullProcessor;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
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
	private Set<String> partNames;
	
	public MemoryIndex(TupleFlowParameters parameters) {
		this(parameters.getXML());
	}

	public MemoryIndex(Parameters parameters) {
		stemming = (boolean) parameters.get("stemming", false);
		int documentNumberOffset = (int) parameters.get("firstDocumentId", 0);
		lastDocId = documentNumberOffset - 1;

		manifest = new MemoryManifest();
		manifest.setOffset(documentNumberOffset);
		documentLengths = new MemoryDocumentLengths(documentNumberOffset);
		documentNames = new MemoryDocumentNames(documentNumberOffset);
		extents = new MemoryExtents();
		postings = new MemoryPostings();
		partNames = new TreeSet<String>();
		partNames.add("postings");
		partNames.add("extents");
		
		if (stemming) {
			partNames.add("stemmedPostings");
			stemmedPostings = new MemoryPostings();
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
					postings.addPosting(doc.terms.get(i), doc.number, i);
				}
			}

			if (stemming) {
				Porter2Stemmer stemmer = new Porter2Stemmer();
				stemmer.setProcessor(new NullProcessor());
				stemmer.process(doc);
				for (int i = 0; i < doc.terms.size(); i++) {
					if (doc.terms.get(i) != null) {
						stemmedPostings.addPosting(doc.terms.get(i),
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

	public NumberedDocumentDataIterator getDocumentNamesIterator() {
		return documentNames.getIterator();
	}

	public NumberedDocumentDataIterator getDocumentLengthsIterator() {
		return documentLengths.getIterator();
	}

	public IndexIterator getPartIterator(String part) throws IOException {
		return getExtentIterator(part);
	}

        public Map<String, NodeType> getPartNodeTypes(String part) throws IOException {

            if (part.equals("extents")) {
		return extents.getNodeTypes();
            }
            if (part.equals("postings")) {
            	return postings.getNodeTypes();
            }
            if (part.equals("stemmedPostings") && stemming) {
		return postings.getNodeTypes();
            }
            throw new IOException("The index has no part named '" + part + "'");
        }

        public NodeType getNodeType(Node node) throws IOException {
            final String operator = node.getOperator();
            NodeType result = null;
            StructuredIndexPartReader part;

            if (operator.equals("extents")) {
		part = extents;
            }
            else if(operator.equals("postings")) {
            	part = postings;
            }
            else if(operator.equals("stemmedPostings") && stemming) {
		part = postings;
            }
            else
                throw new IOException("The index has no part named '" + operator + "'");

            if (part != null) {
                final Map<String, NodeType> nodeTypes = part.getNodeTypes();
                result = nodeTypes.get(operator);
            }
            return result;
        }

        public StructuredIterator getIterator(Node node) throws IOException {
            return getPartIterator(node.getOperator());
        }

	public ExtentIndexIterator getExtentIterator(String part)
			throws IOException {
		if (part.equals("extents")) {
			return extents.getIterator();
		}
		if (part.equals("postings")) {
			return postings.getIterator();
		}
		if (part.equals("stemmedPostings") && stemming) {
			return postings.getIterator();
		}

		return null;
	}

	public Set<String> getPartNames() {
		
		return partNames;
	}

	public boolean isStemmed() {
		return stemming;
	}
}
