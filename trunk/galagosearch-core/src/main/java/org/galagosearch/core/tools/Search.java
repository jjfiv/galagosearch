// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.tools;

import org.galagosearch.core.retrieval.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.SimpleQuery;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.store.NullStore;
import org.galagosearch.core.store.DocumentStore;
import org.galagosearch.core.store.SnippetGenerator;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor, irmarc
 */
public class Search {
  SnippetGenerator generator;
  HashMap<Retrieval, DocumentStore> stores;
  HashMap<String, ArrayList<Retrieval>> retrievals;


  public Search(Retrieval retrieval, DocumentStore store) {
    this.retrievals = new HashMap<String, ArrayList<Retrieval>>();
    ArrayList<Retrieval> defaultList = new ArrayList<Retrieval>();
    defaultList.add(retrieval);	
    this.retrievals.put("all", defaultList);	
    this.stores = new HashMap<Retrieval, DocumentStore>();
    this.stores.put(retrieval, store);

    generator = new SnippetGenerator();
  }

  public Search(Parameters p) {
    this.retrievals = new HashMap<String, ArrayList<Retrieval>>();
    this.retrievals.put("all", new ArrayList<Retrieval>());
    this.stores = new HashMap<Retrieval, DocumentStore>();
    generator = new SnippetGenerator();      

    // Load up the indexes, and for now punt on document stores I think
    DocumentStore store = new NullStore();
    String id, path;
    List<Parameters.Value> indexes = p.list("index");
    for (Parameters.Value value : indexes) {
      id = "all";
      if (value.containsKey("path")) {
        path = value.get("path").toString();
        if (value.containsKey("id")) {
          id = value.get("id").toString();
        }
      } else {
        path = value.toString();
      }
      if (!retrievals.containsKey(id)) retrievals.put(id, new ArrayList<Retrieval>());
      try {
        Retrieval r = Retrieval.instance(path, p);
        retrievals.get(id).add(r);
        if (!id.equals("all")) retrievals.get("all").add(r); // Always put it in default as well
        stores.put(r, store); // for now use the dummy store
      } catch (Exception e) {
        System.err.println("Unable to load index ("+id+") at path " + path + ": " + e.getMessage());
      }
    }
  }

  public void close() throws IOException {
    for (String desc : retrievals.keySet()) {
      ArrayList<Retrieval> collGroup = retrievals.get(desc);
      for (Retrieval r : collGroup) {
        stores.get(r).close();
        r.close();
      }
    }
  }

  public static class SearchResult {
    public Node query;
    public Node transformedQuery;
    public List<SearchResultItem> items;
  }

  public static class SearchResultItem {
    public int rank;
    public int internalId;
    public String identifier;
    public String displayTitle;
    public String url;
    public Map<String, String> metadata;
    public String summary;
    public double score;
  }

  public ArrayList<String> getCollectionGroups() {
    ArrayList<String> collGroups = new ArrayList<String>();
    collGroups.addAll(retrievals.keySet());
    return collGroups;
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

  // ALSO A FILTHY HACK RIGHT NOW
  public Document getDocument(String identifier) throws IOException {
    ArrayList<Retrieval> all = retrievals.get("all");
    DocumentStore store = stores.get(all.get(0));
    return store.get(identifier);
  }

  // FILTHY HACK, NOT SURE HOW TO MAP THIS OTHERWISE RIGHT NOW
  public String getDocumentName(int documentid) throws IOException {
    ArrayList<Retrieval> all = retrievals.get("all");
    return (all.get(0).getDocumentName(documentid));
  }

  public Document getDocument(Retrieval r, String identifier) throws IOException {
    DocumentStore store = stores.get(r);
    return store.get(identifier);
  }

  public String getDocumentName(ArrayList<Retrieval> r, ScoredDocument sd) throws IOException {
    int remoteID = BatchSearch._mapToRemote(sd, r.size());
    return r.get(sd.source).getDocumentName(remoteID);
  }

  public SearchResult runQuery(String query, int startAt, int count, boolean summarize, String descriptor) throws Exception {
    // Try to get the correct subset of retrievals
    if (!retrievals.containsKey(descriptor)) throw new Exception("Unable to load id '" + descriptor +"' for query '"+query+"'");
    ArrayList<Retrieval> subset = retrievals.get(descriptor);
    List<ScoredDocument> scored = new ArrayList<ScoredDocument>();
    Node tree = null;
    Node transformed = null;
    SearchResult result = new SearchResult();
    result.items = new ArrayList();
    tree = parseQuery(query, new Parameters());
    Set<String> queryTerms = StructuredQuery.findQueryTerms(tree);
    result.query = tree;


    // Asynchronous retrieval
    for (int i = 0; i < subset.size(); i++) {
      Retrieval r = subset.get(i);
      if (r.isLocal()) {
        if (transformed == null) {
          transformed = r.transformQuery(tree);
          result.transformedQuery = transformed;
        }
        r.runAsynchronousQuery(transformed, startAt + count, scored, i);
      } else {
        r.runAsynchronousQuery(query, startAt + count, scored, i);
      }
    }

    // Wait for a finished list
    for (Retrieval r : subset) r.join();

    // Map the documents to local
    BatchSearch._mapToLocal(scored, subset.size());

    // Now do all the needed transforms
    for (int i = startAt; i < Math.min(startAt + count, scored.size()); i++) {
      ScoredDocument sd = scored.get(i);
      String identifier = getDocumentName(subset, sd);
      Document document = getDocument(subset.get(sd.source), identifier);
      SearchResultItem item = new SearchResultItem();

      item.rank = i + 1;
      item.identifier = identifier;
      item.displayTitle = identifier;
      item.internalId = sd.document;
      item.score = sd.score;

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
      result.items.add(item);
    }

    return result;
  }
}
