// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.tools;

import org.galagosearch.core.retrieval.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.SimpleQuery;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.store.DocumentStore;
import org.galagosearch.core.store.SnippetGenerator;
import org.galagosearch.tupleflow.Parameters;

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

    public SearchResult runQuery(String query, int startAt, int count, boolean summarize) throws Exception {
        Node tree = parseQuery(query, new Parameters());
        Node transformed = retrieval.transformQuery(tree);
        ScoredDocument[] results = retrieval.runQuery(transformed, startAt + count);
        SearchResult result = new SearchResult();
        Set<String> queryTerms = StructuredQuery.findQueryTerms(tree);
        result.query = tree;
        result.transformedQuery = transformed;
        result.items = new ArrayList();

        for (int i = startAt; i < Math.min(startAt + count, results.length); i++) {
            String identifier = retrieval.getDocumentName(results[i].document);
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
            result.items.add(item);
        }

        return result;
    }
}
