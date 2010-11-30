// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.tools;

import org.galagosearch.core.retrieval.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.galagosearch.core.parse.CorpusReader;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.parse.DocumentIndexReader;
import org.galagosearch.core.parse.DocumentReader;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.SimpleQuery;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.retrieval.structured.StructuredRetrieval;
import org.galagosearch.core.store.DocumentIndexStore;
import org.galagosearch.core.store.DocumentStore;
import org.galagosearch.core.store.NullStore;
import org.galagosearch.core.store.SnippetGenerator;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Parameters.Value;

/**
 *
 * @author trevor
 */
public class Search {

  SnippetGenerator generator;
  DocumentStore store;
  Retrieval retrieval;

  public Search(Retrieval retrieval, DocumentStore store) {
    this.store = store;
    this.retrieval = retrieval;
    generator = new SnippetGenerator();
  }

  public Search(Parameters params) throws IOException {
    this.store = getDocumentStore(params.list("corpus"));
    this.retrieval = new StructuredRetrieval(params.get("index"), new Parameters());
    generator = new SnippetGenerator();
  }

  private DocumentStore getDocumentStore(List<Value> corpora) throws IOException {
    DocumentStore store = null;
    if (corpora.size() > 0) {
      ArrayList<DocumentReader> readers = new ArrayList<DocumentReader>();
      for (Value corpus : corpora) {
        String c = corpus.toString();
        if (CorpusReader.isCorpus(c)) {
          readers.add(new CorpusReader(c));
        } else {
          readers.add(new DocumentIndexReader(c));
        }
      }
      store = new DocumentIndexStore(readers);
    } else {
      store = new NullStore();
    }
    return store;
  }

  public void close() throws IOException {
    store.close();
    retrieval.close();
  }

  public static class SearchResult {

    public Node query;
    public Node transformedQuery;
    public List<SearchResultItem> items;
  }

  public static class SearchResultItem {

    public int rank;
    public String identifier;
    public String displayTitle;
    public String url;
    public Map<String, String> metadata;
    public String summary;
    public double score;
  }

  public String getSummary(Document document, Set<String> query) throws IOException {
    if (document.metadata.containsKey("description")) {
      String description = document.metadata.get("description");

      if (description.length() > 10) {
        return generator.highlight(description, query);
      }
    }

    return generator.getSnippet(document.text, query);
  }

  public static Node parseQuery(String query, Parameters parameters) {
    String queryType = parameters.get("queryType", "complex");

    if (queryType.equals("simple")) {
      return SimpleQuery.parseTree(query);
    }

    return StructuredQuery.parse(query);
  }

  public Document getDocument(String identifier) throws IOException {
    return store.get(identifier);
  }

  public long xcount(String nodeString) throws Exception {
      return this.retrieval.xcount(nodeString);
  }

  public SearchResult runQuery(String query, int startAt, int count, boolean summarize, String id) throws Exception {
    Parameters p = new Parameters();
    p.add("indexId", "0");
    p.add("requested", Integer.toString(startAt + count));
    ScoredDocument[] results = retrieval.runQuery(query, p);
    SearchResult result = new SearchResult();

    Node tree = parseQuery(query, new Parameters());
    Set<String> queryTerms = StructuredQuery.findQueryTerms(tree);
    result.query = tree;
    result.items = new ArrayList();

    for (int i = startAt; i < Math.min(startAt + count, results.length); i++) {
      String identifier = results[i].documentName;
      Document document = getDocument(identifier);
      SearchResultItem item = new SearchResultItem();

      item.rank = i + 1;
      item.identifier = identifier;
      item.displayTitle = identifier;

      if (document.metadata.containsKey("title")) {
        item.displayTitle = document.metadata.get("title");
      }

      if (item.displayTitle != null) {
        item.displayTitle = generator.highlight(item.displayTitle, queryTerms);
      }

      if (document.metadata.containsKey("url")) {
        item.url = document.metadata.get("url");
      }

      if (summarize) {
        item.summary = getSummary(document, queryTerms);
      }

      item.metadata = document.metadata;
      item.score = results[i].score;
      result.items.add(item);
    }

    return result;
  }
}
