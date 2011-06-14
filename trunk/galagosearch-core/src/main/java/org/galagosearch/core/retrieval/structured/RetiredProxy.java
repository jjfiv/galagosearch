// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.tupleflow.Parameters;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.galagosearch.core.retrieval.query.StructuredQuery;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author irmarc
 */
public class RetiredProxy implements Retrieval {

  String indexUrl;
  private SAXParser parser;
  // For asynchronous evaluation
  Thread runner;
  Node query;
  Parameters queryParams;
  List<ScoredDocument> queryResults;

  public RetiredProxy(String url, Parameters parameters) throws IOException {
    this.indexUrl = url;
    this.parser = null;
  }

  public void close() throws IOException {
    // Nothing to do - index is serving remotely - possibly to several handlers
  }


  /* this function should return:
   *
   * <parameters>
   *  <collectionLength>cl<collectionLength>
   *  <documentCount>dc<documentCount>
   *  <part>
   *   <partName>n</partName>
   *   (<nodeType>n</nodeType>) +
   *  </part>
   * </parameters>
   */
  public Parameters getRetrievalStatistics() throws IOException {
    return getRetrievalStatistics("all");
  }

  public Parameters getRetrievalStatistics(String retGroup) throws IOException {
    StringBuilder request = new StringBuilder(indexUrl);
    String encoded = URLEncoder.encode(query.toString(), "UTF-8"); // need to web-escape
    request.append("/stats?retGroup=").append(retGroup);

    URL resource = new URL(request.toString());
    HttpURLConnection connection = (HttpURLConnection) resource.openConnection();
    connection.setRequestMethod("GET");

    InputStream stream = connection.getInputStream();
    // now parse the stream -> parameter object
    ByteArrayOutputStream array = new ByteArrayOutputStream();
    int d = stream.read();
    while (d >= 0) {
      array.write(d);
      d = stream.read();
    }
    connection.disconnect();

    return new Parameters(array.toByteArray());
  }

  /* this function should return:
   *
   * <parameters>
   *  <collectionLength>cl<collectionLength>
   *  <documentCount>dc<documentCount>
   *  <part>
   *   <partName>n</partName>
   *   (<nodeType>n</nodeType>) +
   *  </part>
   * </parameters>
   */
  public Parameters getAvailableParts(String retGroup) throws IOException {
    StringBuilder request = new StringBuilder(indexUrl);
    String encoded = URLEncoder.encode(query.toString(), "UTF-8"); // need to web-escape
    request.append("/parts?retGroup=").append(retGroup);

    URL resource = new URL(request.toString());
    HttpURLConnection connection = (HttpURLConnection) resource.openConnection();
    connection.setRequestMethod("GET");

    InputStream stream = connection.getInputStream();
    // now parse the stream -> parameter object
    ByteArrayOutputStream array = new ByteArrayOutputStream();
    int d = stream.read();
    while (d >= 0) {
      array.write(d);
      d = stream.read();
    }
    connection.disconnect();

    return new Parameters(array.toByteArray());
  }

  public StructuredIterator createIterator(Node node) throws Exception {
    throw new UnsupportedOperationException("cannot create proxy iterator");
  }

  public ScoredDocument[] runRankedQuery(Node root, Parameters parameters) throws Exception {

    int requested = (int) parameters.get("requested", 1000);
    String qtype = parameters.get("queryType", "complex");
    String indexId = parameters.get("indexId", "0");
    String subset = parameters.get("retrievalGroup", "all");

    ArrayList<ScoredDocument> results = new ArrayList<ScoredDocument>();

    StringBuilder request = new StringBuilder(indexUrl);
    String encoded = URLEncoder.encode(root.toString(), "UTF-8"); // need to web-escape
    request.append("/searchxml?q=").append(encoded);
    request.append("&n=").append(requested);
    request.append("&start=").append(0);
    request.append("&qtype=").append(qtype);
    request.append("&indexId=").append(indexId);
    request.append("&subset=").append(subset);
    request.append("&transform=").append("false"); // all runquery queries should be pre-transformed by search/batch-search

    URL resource = new URL(request.toString());
    HttpURLConnection connection = (HttpURLConnection) resource.openConnection();
    connection.setRequestMethod("GET");

    // Hook up an xml handler to the input stream to directly generate the results, as opposed
    // to buffering them up
    if (parser == null) {
      parser = SAXParserFactory.newInstance().newSAXParser();
    }

    // might be a better way to do this....
    SearchResultHandler handler = new SearchResultHandler();
    handler.reset();
    parser.parse(connection.getInputStream(), handler);
    connection.disconnect();
    return (handler.getResults());
  }

  public Node transformRankedQuery(Node queryTree, String retrievalGroup) throws Exception {
    String query = queryTree.toString();
    StringBuilder request = new StringBuilder(indexUrl);
    String encoded = URLEncoder.encode(query, "UTF-8"); // need to web-escape
    request.append("/transform?q=").append(encoded);
    request.append("&retrievalGroup=").append(retrievalGroup);

    URL resource = new URL(request.toString());
    HttpURLConnection connection = (HttpURLConnection) resource.openConnection();
    connection.setRequestMethod("GET");

    // Hook up an xml handler to the input stream to directly generate the results, as opposed
    // to buffering them up
    if (parser == null) {
      parser = SAXParserFactory.newInstance().newSAXParser();
    }

    // might be a better way to do this....
    TransformQueryHandler handler = new TransformQueryHandler();
    handler.reset();
    parser.parse(connection.getInputStream(), handler);
    connection.disconnect();
    Node root = StructuredQuery.parse(handler.nodeString);
    return (root);
  }

  public void runAsynchronousQuery(Node query, Parameters parameters, List<ScoredDocument> queryResults, List<String> errors) throws Exception {
    this.query = query;
    this.queryParams = parameters;
    this.queryResults = queryResults;

    runner = new Thread(this);
    runner.start();
  }

  public void waitForAsynchronousQuery() throws InterruptedException {
    this.join();
  }

  public void join() throws InterruptedException {
    if (runner != null) {
      runner.join();
    }
    runner = null;
  }

  public void run() {
    try {
      ScoredDocument[] docs = runRankedQuery(query, queryParams);

      // Now add it to the accumulator, but synchronously
      synchronized (queryResults) {
        queryResults.addAll(Arrays.asList(docs));
      }
    } catch (Exception e) {
      System.err.println("ERROR RETRIEVING: " + e.getMessage());
    }
  }

  public ScoredDocument[] runParameterSweep(Node root, Parameters parameters) throws Exception {
    throw new UnsupportedOperationException("Parameter Sweep not yet implemented");
  }

  @Override
  public long xCount(String nodeString) throws Exception {

    StringBuilder request = new StringBuilder(indexUrl);
    String encoded = URLEncoder.encode(nodeString, "UTF-8"); // need to web-escape
    request.append("/xcount?expression=").append(encoded);
    URL resource = new URL(request.toString());
    HttpURLConnection connection = (HttpURLConnection) resource.openConnection();
    connection.setRequestMethod("GET");

    // Hook up an xml handler to the input stream to directly generate the results, as opposed
    // to buffering them up
    if (parser == null) {
      parser = SAXParserFactory.newInstance().newSAXParser();
    }

    // might be a better way to do this....
    XCountHandler handler = new XCountHandler();
    parser.parse(connection.getInputStream(), handler);
    connection.disconnect();
    return (handler.getCount());
  }

  public long xCount(Node root) throws Exception {
    return xCount(root.toString());
  }

  public long docCount(Node root) throws Exception {
    return docCount(root.toString());
  }

  public long docCount(String nodeString) throws Exception {

    StringBuilder request = new StringBuilder(indexUrl);
    String encoded = URLEncoder.encode(nodeString, "UTF-8"); // need to web-escape
    request.append("/doccount?expression=").append(encoded);
    URL resource = new URL(request.toString());
    HttpURLConnection connection = (HttpURLConnection) resource.openConnection();
    connection.setRequestMethod("GET");

    // Hook up an xml handler to the input stream to directly generate the results, as opposed
    // to buffering them up
    if (parser == null) {
      parser = SAXParserFactory.newInstance().newSAXParser();
    }

    // might be a better way to do this....
    DocCountHandler handler = new DocCountHandler();
    parser.parse(connection.getInputStream(), handler);
    connection.disconnect();
    return (handler.getCount());
  }

  // this function is for the query transform function (which should not be completed here)
  @Override
  public NodeType getNodeType(Node node, String retrievalGroup) throws Exception {
    throw new UnsupportedOperationException("Not supported and never will be.");
  }

  @Override
  public ScoredDocument[] runBooleanQuery(Node root, Parameters parameters) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Node transformBooleanQuery(Node root, String retrievalGroup) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public Node transformCountQuery(Node root, String retrievalGroup) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  // private classes
  private static class StatisticsResultHandler extends DefaultHandler {

    Stack<String> contexts;
    Parameters parameters;

    public StatisticsResultHandler() {
      contexts = new Stack<String>();
      parameters = new Parameters();
    }

    public void reset() {
      parameters = new Parameters();
      contexts.clear();
    }

    public void endElement(String uri, String localName, String rawName) {
      contexts.pop();
    }

    public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws SAXException {
      contexts.push(qName);
    }

    public void characters(char[] ch, int start, int length) {
      String context = contexts.peek();
      if (context.equals("collectionLength")) {
        String value = new String(ch, start, length);
        parameters.add("collectionLength", value);
      } else if (context.equals("documentCount")) {
        String value = new String(ch, start, length);
        parameters.add("documentCount", value);
      } else if (context.equals("part")) {
        String value = new String(ch, start, length);
        parameters.add("part", value);
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
      } else if (context.equals("name")) {
        String value = new String(ch, start, length);
        results.get(results.size() - 1).documentName = value;
      } else if (context.equals("index")) {
        String value = new String(ch, start, length);
        results.get(results.size() - 1).source = value;
      }
    }
  }

  private static class XCountHandler extends DefaultHandler {

    String context;
    long value;

    public XCountHandler() {
      reset();
    }

    public long getCount() {
      return value;
    }

    public void reset() {
      value = 0;
      context = null;
    }

    public void endElement(String uri, String localName, String rawName) {
      context = null;
    }

    public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws SAXException {
      context = qName;
    }

    public void characters(char[] ch, int start, int length) {
      if (context.equals("count")) {
        value = Integer.parseInt(new String(ch, start, length));
        String value = new String(ch, start, length);
      }
    }
  }

  private static class DocCountHandler extends DefaultHandler {

    String context;
    long value;

    public DocCountHandler() {
      reset();
    }

    public long getCount() {
      return value;
    }

    public void reset() {
      value = 0;
      context = null;
    }

    public void endElement(String uri, String localName, String rawName) {
      context = null;
    }

    public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws SAXException {
      context = qName;
    }

    public void characters(char[] ch, int start, int length) {
      if (context.equals("count")) {
        value = Integer.parseInt(new String(ch, start, length));
        String value = new String(ch, start, length);
      }
    }
  }

  private static class TransformQueryHandler extends DefaultHandler {

    String context;
    public String nodeString;

    public TransformQueryHandler() {
      reset();
    }

    public void reset() {
      nodeString = "";
      context = null;
    }

    public void endElement(String uri, String localName, String rawName) {
      context = null;
    }

    public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws SAXException {
      context = qName;
    }

    public void characters(char[] ch, int start, int length) {
      if (context.equals("query")) {
        nodeString = new String(ch, start, length);
      }
    }
  }
}
