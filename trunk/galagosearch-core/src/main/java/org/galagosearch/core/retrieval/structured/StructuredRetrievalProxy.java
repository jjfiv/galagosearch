// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.tupleflow.Parameters;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author irmarc
 */
public class StructuredRetrievalProxy extends Retrieval {

  // For asynchronous evaluation
  Thread runner;
  String query;
  List<ScoredDocument> scored;
  int resultsRequested;
  int idx;

  public static class DocumentNameMapping {

    public int document;
    public String name;
  }

  private static class DocumentNameHandler extends DefaultHandler {

    Stack<String> contexts;
    ArrayList<DocumentNameMapping> results;

    public DocumentNameHandler() {
      contexts = new Stack<String>();
      results = new ArrayList<DocumentNameMapping>();
    }

    public void reset() {
      results.clear();
      contexts.clear();
    }

    public DocumentNameMapping[] getResults() {
      return (results.toArray(new DocumentNameMapping[0]));
    }

    public void endElement(String uri, String localName, String rawName) {
      contexts.pop();
    }

    public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws SAXException {
      contexts.push(qName);
      if (qName.equals("result")) {
        results.add(new DocumentNameMapping());
      }
    }

    public void characters(char[] ch, int start, int length) {
      String context = contexts.peek();
      if (context.equals("id")) {
        String value = new String(ch, start, length);
        results.get(results.size() - 1).document = Integer.parseInt(value);
      } else if (context.equals("name")) {
        String value = new String(ch, start, length);
        results.get(results.size() - 1).name = value;
      }
    }
  }

  private static class SearchResultHandler extends DefaultHandler {

    Stack<String> contexts;
    ArrayList<ScoredDocument> results;

    public SearchResultHandler() {
      contexts = new Stack<String>();
      results = new ArrayList<ScoredDocument>();
    }

    public void reset() {
      results.clear();
      contexts.clear();
    }

    public ScoredDocument[] getResults() {
      return (results.toArray(new ScoredDocument[0]));
    }

    public void endElement(String uri, String localName, String rawName) {
      contexts.pop();
    }

    public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws SAXException {
      contexts.push(qName);
      if (qName.equals("result")) {
        results.add(new ScoredDocument());
      }
    }

    public void characters(char[] ch, int start, int length) {
      String context = contexts.peek();
      if (context.equals("document")) {
        String value = new String(ch, start, length);
        int docid = Integer.parseInt(value);
        results.get(results.size() - 1).document = docid;
      } else if (context.equals("score")) {
        String value = new String(ch, start, length);
        double score = Double.parseDouble(value);
        results.get(results.size() - 1).score = score;
      }
    }
  }
  private String url;
  private SAXParser parser = null;

  public StructuredRetrievalProxy(String url, Parameters parameters) throws IOException {
    this.url = url;
  }

  public boolean isLocal() {
    return false;
  }

  public String getDocumentName(int document) throws IOException {
    ArrayList<Integer> wrapper = new ArrayList<Integer>();
    wrapper.add(document);
    String[] results = getDocumentNames(wrapper);
    return (results[0]);
  }

  public String[] getDocumentNames(List<Integer> documents) throws IOException {
    StringBuilder request = new StringBuilder(url);
    request.append("/documentnames?ids=");
    for (int i = 0; i < documents.size(); i++) {
      if (i != 0) {
        request.append(",");
      }
      request.append(Integer.toString(documents.get(i)));
    }

    try {
      URL resource = new URL(request.toString());
      HttpURLConnection connection = (HttpURLConnection) resource.openConnection();
      connection.setRequestMethod("GET");

      if (parser == null) {
        parser = SAXParserFactory.newInstance().newSAXParser();
      }
      DocumentNameHandler handler = new DocumentNameHandler();
      handler.reset();
      parser.parse(connection.getInputStream(), handler);
      connection.disconnect();
      DocumentNameMapping[] results = handler.getResults();
      String[] names = new String[results.length];
      for (int i = 0; i < results.length; i++) {
        names[i] = results[i].name;
      }
      return (names);
    } catch (SAXException saxe) {
      throw new IOException(saxe);
    } catch (ParserConfigurationException pce) {
      throw new IOException(pce);
    }
  }

  public Node transformQuery(Node query) throws Exception {
    throw new Exception("Not implemented yet");
  }

  public ScoredDocument[] runQuery(Node query, int requested) throws Exception {
    throw new Exception("Not handling this yet");
  }

  public ScoredDocument[] runQuery(String query, int requested) throws Exception {
    ArrayList<ScoredDocument> results = new ArrayList<ScoredDocument>();
    StringBuilder request = new StringBuilder(url);
    String encoded = URLEncoder.encode(query, "UTF-8");
    request.append("/searchxml?q=").append(encoded); // need to web-escape
    request.append("&n=").append(requested);
    request.append("&start=").append(0);

    URL resource = new URL(request.toString());
    HttpURLConnection connection = (HttpURLConnection) resource.openConnection();
    connection.setRequestMethod("GET");

    // Hook up an xml handler to the input stream to directly generate the results, as opposed
    // to buffering them up
    if (parser == null) {
      parser = SAXParserFactory.newInstance().newSAXParser();
    }
    SearchResultHandler handler = new SearchResultHandler();
    handler.reset();
    parser.parse(connection.getInputStream(), handler);
    connection.disconnect();
    return (handler.getResults());
  }

  public void close() throws IOException {
    // Nothing to do
  }

  // To do asynchronous retrieval
  public void runAsynchronousQuery(Node query, int requested,
          List<ScoredDocument> scored, int idx) throws Exception {
    throw new Exception("Unimplemented in this form.");
  }

  public void runAsynchronousQuery(String query, int requested,
          List<ScoredDocument> scored, int idx) throws Exception {
    this.query = query;
    this.resultsRequested = requested;
    this.scored = scored;
    this.idx = idx;
    runner = new Thread(this);
    runner.start();
  }

  public void join() throws InterruptedException {
    if (runner != null) {
      runner.join();
    }
    runner = null;
  }

  public void run() {
    try {
      ScoredDocument[] docs = runQuery(query, resultsRequested);
      for (ScoredDocument sd : docs) {
        sd.source = idx;
      }

      // Now add it to the accumulator, but synchronously
      synchronized (scored) {
        scored.addAll(Arrays.asList(docs));
      }
    } catch (Exception e) {
      System.err.println("ERROR RETRIEVING: " + e.getMessage());
    }
  }
}
