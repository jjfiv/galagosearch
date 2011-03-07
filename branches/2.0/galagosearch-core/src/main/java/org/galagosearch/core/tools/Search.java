// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.tools;

import org.galagosearch.core.retrieval.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.galagosearch.core.index.corpus.CorpusReader;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.index.corpus.DocumentIndexReader;
import org.galagosearch.core.index.corpus.DocumentReader;
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

  public Search(Parameters params) throws Exception {
    this.store = getDocumentStore(params.list("corpus"));
    this.retrieval = Retrieval.instance(params);
    generator = new SnippetGenerator();
  }

  private DocumentStore getDocumentStore(List<Value> corpora) throws IOException {
    DocumentStore store = null;
    if (corpora.size() > 0) {
      ArrayList<DocumentReader> readers = new ArrayList<DocumentReader>();
      for (Value corpus : corpora) {
        String c = corpus.toString();
        readers.add( DocumentReader.getInstance(c) );
      }
      store = new DocumentIndexStore(readers);
    } else {
      store = new NullStore();
    }
    return store;
  }

  public Parameters getRetrievalStats(String retGroup) throws IOException {
    return retrieval.getRetrievalStatistics(retGroup);
  }

  public Parameters getAvailiableParts(String retGroup) throws IOException{
    return retrieval.getAvailableParts(retGroup);
  }

  public void close() throws IOException {
    store.close();
    retrieval.close();
  }

  public static class SearchResult {
    public String queryAsString;
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

  public long doccount(String nodeString) throws Exception {
    return this.retrieval.doccount(nodeString);
  }

  public SearchResult runQuery(String query, Parameters p, boolean summarize) throws Exception {
    Node root = StructuredQuery.parse(query);
    Node transformed = retrieval.transformQuery(root, p.get("retrievalGroup","all"));
    SearchResult result = runTransformedQuery(transformed, p, summarize);
    result.query = root;
    result.queryAsString = query;
    return result;
  }

  public SearchResult runTransformedQuery(Node root, Parameters p, boolean summarize) throws Exception {
    int startAt = Integer.parseInt(p.get("startAt"));
    int count = Integer.parseInt(p.get("resultCount"));

    ScoredDocument[] results = retrieval.runQuery(root, p);
    SearchResult result = new SearchResult();
    Set<String> queryTerms = StructuredQuery.findQueryTerms(root);
    result.transformedQuery = root;
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
