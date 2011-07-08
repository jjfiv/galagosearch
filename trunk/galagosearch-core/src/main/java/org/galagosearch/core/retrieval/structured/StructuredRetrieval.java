// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.TreeMap;
import org.galagosearch.core.index.DocumentLengthsReader;
import org.galagosearch.core.index.NameReader;
import org.galagosearch.core.index.PositionIndexReader;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.core.retrieval.featurefactory.CountFeatureFactory;
import org.galagosearch.core.retrieval.featurefactory.BooleanFeatureFactory;
import org.galagosearch.core.retrieval.featurefactory.RankedFeatureFactory;
import org.galagosearch.core.retrieval.featurefactory.FeatureFactory;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.query.SimpleQuery;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.util.CallTable;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;
import java.io.File;
import java.io.FileFilter;

/**
 * 10/7/2010 - Modified for asynchronous execution
 *
 * @author trevor
 * @author irmarc
 */
public class StructuredRetrieval implements Retrieval {

  protected String indexId;
  protected Map<String, StructuredIndex> index;
  // feature factories for each type of query
  protected Map<String, FeatureFactory> booleanFeatureFactory;
  protected Map<String, FeatureFactory> countFeatureFactory;
  protected Map<String, FeatureFactory> rankedFeatureFactory;
  // these allow asynchronous evaluation
  protected Thread runner;
  protected Node query;
  protected Parameters queryParams;
  protected List<ScoredDocument> queryResults;
  protected List<String> errors;
  protected String default_index = null;

  /**
   * Constructors for StructuredRetrieval: We now can take a single index, an array 
   * of indices, or a path filename. All are converted to a hashmap of one or more 
   * StructuredIndex indices (keyed on the string specified by the folder containing 
   * the parts of each index.
   */
  public StructuredRetrieval(StructuredIndex index, Parameters factoryParameters) throws IOException {
    this.index = new HashMap<String, StructuredIndex>();
    this.booleanFeatureFactory = new HashMap<String, FeatureFactory>();
    this.countFeatureFactory = new HashMap<String, FeatureFactory>();
    this.rankedFeatureFactory = new HashMap<String, FeatureFactory>();

    default_index = addIndex(index, factoryParameters);

    runner = null;
  }

  public StructuredRetrieval(StructuredIndex[] indexes, Parameters factoryParameters) throws IOException {
    this.index = new HashMap<String, StructuredIndex>();
    this.booleanFeatureFactory = new HashMap<String, FeatureFactory>();
    this.countFeatureFactory = new HashMap<String, FeatureFactory>();
    this.rankedFeatureFactory = new HashMap<String, FeatureFactory>();

    for (StructuredIndex indx : indexes) {
      String key = addIndex(indx, factoryParameters);
      if (default_index == null) {
        default_index = key;
      }
    }
    runner = null;
  }

  /** 
   * For this constructor, being sent a filename path to the indicies, 
   * we first list out all the directories in the path. If there are none, then 
   * we can safely assume that the filename specifies a single index (the files 
   * listed are all parts), otherwise we will treat each subdirectory as a 
   * separate logical index.
   */
  public StructuredRetrieval(String filename, Parameters parameters)
          throws FileNotFoundException, IOException {

    this.index = new HashMap<String, StructuredIndex>();
    this.booleanFeatureFactory = new HashMap<String, FeatureFactory>();
    this.countFeatureFactory = new HashMap<String, FeatureFactory>();
    this.rankedFeatureFactory = new HashMap<String, FeatureFactory>();

    default_index = addIndex(new StructuredIndex(filename), parameters);

    runner = null;

    /*  [sjh] -- this code isn't doing anything 
     *        -- i'd prefer a different system of identifying a set of indexes.
     * 
    File dir = new File(filename);
    // This filter only returns directories
    FileFilter fileFilter = new FileFilter() {
    public boolean accept(File file) {
    return file.isDirectory();
    }
    };
    File[] files = dir.listFiles(fileFilter);
    
    // This doesn't work for the way we normally strcuture indexes.
    if(files.length < 2) {
    if (true) {
    default_index = addIndex(new StructuredIndex(filename), parameters);
    }
    
    else {
        for(File file : files)
        {
            String key = addIndex(new StructuredIndex(file.toString()), parameters);
            if(default_index == null)
                default_index = key;
        }
    }
     * 
     */
  }

  /**
   * Here we do the work of processing each separate logical index. 
   */
  private String addIndex(StructuredIndex indx, Parameters parameters) throws IOException {
    // Get the path to the index, and extract its 'name' which is its key
    String[] indexPath = indx.getIndexLocation().toString().split("/");
    String index_key = indexPath[indexPath.length - 1];
    System.out.println("Logical Index: " + index_key);

    // Insert it into the hash map
    this.index.put(index_key, indx);

    // Handle parameters for this index (since some of these can be different)
    Parameters featureParameters = parameters.clone();
    Parameters indexStats = getRetrievalStatistics("all", index_key);

    featureParameters.add("collectionLength", indexStats.get("collectionLength"));
    featureParameters.add("documentCount", indexStats.get("documentCount"));
    featureParameters.add("retrievalGroup", "all"); // the value wont matter here

    Parameters bfp = featureParameters.clone();
    bfp.set("queryType", "boolean");
    booleanFeatureFactory.put(index_key, new BooleanFeatureFactory(bfp));

    Parameters cfp = featureParameters.clone();
    cfp.set("queryType", "count");
    countFeatureFactory.put(index_key, new CountFeatureFactory(cfp));

    Parameters rfp = featureParameters.clone();
    rfp.set("queryType", "ranked");
    rankedFeatureFactory.put(index_key, new RankedFeatureFactory(rfp));

    return index_key;
  }

  // This method and others have been adapted to multiple logical indicies
  public void close() throws IOException {
    for (StructuredIndex indx : this.index.values()) {
      indx.close();
    }
  }

  /*
   * <parameters>
   *  <collectionLength>cl<collectionLength>
   *  <documentCount>dc<documentCount>
   * </parameters>
   */
  public Parameters getRetrievalStatistics() throws IOException {
    return getRetrievalStatisticsForIndex(default_index);
  }

  /**
   * When no index key is given we revert to the default, which is the first index 
   * specified in the list. Thus this is backwards compatible if only one 
   * index is given.
   */
  public Parameters getRetrievalStatistics(String _retGroup) throws IOException {
    return getRetrievalStatistics();
  }

  public Parameters getRetrievalStatistics(String _retGroup, String index_key) throws IOException {
    return getRetrievalStatisticsForIndex(index_key);
  }

  public Parameters getRetrievalStatisticsForIndex(String index_key) throws IOException {
    Parameters p = new Parameters();
    if (!index.containsKey(index_key)) {
      throw new IOException("Index requested was not found to be loaded!!");
    }

    p.add("collectionLength", Long.toString(index.get(index_key).getCollectionLength()));
    p.add("documentCount", Long.toString(index.get(index_key).getDocumentCount()));

    for (String part : index.get(index_key).getPartNames()) {
      p.copy(index.get(index_key).getPartStatistics(part));
    }

    return p;
  }

  /*
   * <parameters>
   *  <part>
   *   (partName)+
   *  </part>
   *  <nodeType>
   *   <(partName)_(nodeType)>(class)</(partName)_(nodeType)>
   *  </nodeType>
   * </parameters>
   */
  public Parameters getAvailableParts(String _retGroup) throws IOException {
    return getAvailableParts(_retGroup, default_index);
  }

  public Parameters getAvailableParts(String _retGroup, String index_key) throws IOException {
    Parameters p = new Parameters();
    if (!index.containsKey(index_key)) {
      throw new IOException("Index requested was not found to be loaded!!");
    }

    for (String partName : index.get(index_key).getPartNames()) {
      p.add("part", partName);

      Map<String, NodeType> nodeTypes = index.get(index_key).getPartNodeTypes(partName);
      for (String nodeType : nodeTypes.keySet()) {
        p.add("nodeType/" + partName + "/" + nodeType, nodeTypes.get(nodeType).getIteratorClass().getName());
      }
    }
    return p;
  }

  public ScoredDocument[] runBooleanQuery(Node queryTree, Parameters parameters) throws Exception {
    // Give it a context
    DocumentContext context = new DocumentContext();

    // construct the query iterators
    String index_key = queryTree.getIndexTarget(default_index);
    AbstractIndicator iterator = (AbstractIndicator) createIterator(queryTree, context, booleanFeatureFactory.get(index_key));
    ArrayList<ScoredDocument> list = new ArrayList<ScoredDocument>();
    while (!iterator.isDone()) {
      if (iterator.getStatus()) {
        list.add(new ScoredDocument(iterator.currentCandidate(), 1.0));
      }
      iterator.next();
    }
    return list.toArray(new ScoredDocument[0]);
  }

  public DocumentLengthsReader.KeyIterator getLengthsIterator(String index_key) throws IOException {
    if (!index.containsKey(index_key)) {
      throw new IOException("Index requested was not found to be loaded!!");
    }

    return index.get(index_key).getLengthsIterator();
  }

  /**
   * Evaluates a probabilistic query using document-at-a-time evaluation.
   *
   * @param query A query tree that has been already transformed with StructuredRetrieval.transformRankedQuery.
   * @param parameters - query parameters (indexId, # requested, query type, transform)
   * @return
   * @throws java.lang.Exception
   */
  public ScoredDocument[] runRankedQuery(Node queryTree, Parameters parameters) throws Exception {

    long start = System.currentTimeMillis();

    // Give it a context
    DocumentContext context = ContextFactory.createContext(parameters);

    // construct the query iterators
    String index_key = queryTree.getIndexTarget(default_index);
    ScoreValueIterator iterator = (ScoreValueIterator) createIterator(queryTree, context, rankedFeatureFactory.get(index_key));
    int requested = (int) parameters.get("requested", 1000);
    //System.err.printf("Running ranked query (%d) %s\n", requested, queryTree.toString());

    // now there should be an iterator at the root of this tree
    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>();

    /* Another change to use multiple logical indices: 
     * We look at the queryTree (node) for what index it targets (uses). 
     * This is required, but again we default appropriately if nothing is specified.
     * Not every query node must specify the target. Only leaf nodes, which are 
     * terms and thus need to know which index to search for that term in, and 
     * convert operators (which need to know what to conver to) need to specify this. 
     * The rest figure it out automatically by looking at their child nodes.
     */
    if (!index.containsKey(index_key)) {
      throw new IOException("Index requested (in runRankedQuery) was not found to be loaded!!");
    }

    DocumentLengthsReader.KeyIterator lengthsIterator = index.get(index_key).getLengthsIterator();

    while (!iterator.isDone()) {
      int document = iterator.currentCandidate();
      if (iterator.hasMatch(document)) {
        lengthsIterator.skipToKey(document);
        int length = lengthsIterator.getCurrentLength();
        // This context is shared among all scorers
        context.document = document;
        context.length = length;
        double score = iterator.score();
        CallTable.increment("scored");
        if (requested < 0 || queue.size() <= requested || queue.peek().score < score) {
          ScoredDocument scoredDocument = new ScoredDocument(document, score);
          queue.add(scoredDocument);
          if (requested > 0 && queue.size() > requested) {
            queue.poll();
          }
        }
      }
      iterator.next();
    }
    long runtime = System.currentTimeMillis() - start;
    CallTable.increment("realtime", runtime);
    String indexId = parameters.get("indexId", "0");
    return getArrayResults(queue, indexId, index_key);
  }

  /**
   *
   * @param query - query to be evaluated
   * @param parameters - query parameters (indexId, # requested, query type, transform, retrievalGroup)
   * @param queryResults - object that will contain the results
   * @throws Exception
   */
  public void runAsynchronousQuery(Node query, Parameters parameters, List<ScoredDocument> queryResults, List<String> errors) throws Exception {
    this.query = query;
    this.queryParams = parameters;
    this.queryResults = queryResults;
    this.errors = errors;

    System.err.println();

    runner = new Thread(this);
    runner.start();
  }

  public void waitForAsynchronousQuery() throws InterruptedException {
    this.join();
  }

  // Finish and clean up
  public void join() throws InterruptedException {
    if (runner != null) {
      runner.join();
    }
    query = null;
    runner = null;
  }

  public void run() {
    // we haven't got a query to run - return
    if (query == null) {
      return;
    }

    try {
      // use the query parameters to determine the type of query we're running
      String runType = queryParams.get("querytype", "ranked");
      ScoredDocument[] results;
      if (runType.equals("boolean")) {
        results = runBooleanQuery(query, queryParams);
      } else {
        results = runRankedQuery(query, queryParams);
      }

      // Now add it to the output structure, but synchronously
      synchronized (queryResults) {
        queryResults.addAll(Arrays.asList(results));
      }
    } catch (Exception e) {
      // TODO: use logger here
      System.err.println("StructuredRetrieval ERROR RETRIEVING: " + e);
      e.printStackTrace(System.err);
      synchronized (errors) {
        errors.add(e.toString());
      }
    }
  }

  /*
   * getArrayResults annotates a queue of scored documents
   * returns an array
   *
   */
  protected ScoredDocument[] getArrayResults(PriorityQueue<ScoredDocument> scores, String indexId, String index_key) throws IOException {
    ScoredDocument[] results = new ScoredDocument[scores.size()];

    TreeMap<Integer, Integer> docIds = new TreeMap();

    for (int i = scores.size() - 1; i >= 0; i--) {
      results[i] = scores.poll();
      results[i].source = indexId;
      results[i].rank = i + 1;
      //results[i].documentName = getName(results[i].intID);
      docIds.put(results[i].document, i);
    }

    if (!index.containsKey(index_key)) {
      throw new IOException("Index requested (in getArrayResults) was not found to be loaded!!");
    }

    NameReader.Iterator iterator = index.get(index_key).getNamesIterator();
    for (int document : docIds.keySet()) {
      iterator.findKey(Utility.fromInt(document));
      String name = iterator.getValueString();
      results[docIds.get(document)].documentName = name;
    }

    return results;
  }

  protected String getDocumentName(int document, String index_key) throws IOException {
    if (!index.containsKey(index_key)) {
      throw new IOException("Index requested was not found to be loaded!!");
    }

    return index.get(index_key).getName(document);
  }

  protected Node parseQuery(String query, Parameters parameters) {
    String queryType = parameters.get("queryType", "complex");

    if (queryType.equals("simple")) {
      return SimpleQuery.parseTree(query);
    }

    return StructuredQuery.parse(query);
  }

  public StructuredIterator createIterator(Node node, DocumentContext context) throws Exception {
    return createIterator(node, context, this.rankedFeatureFactory.get(node.getIndexTarget(default_index)));
  }

  protected StructuredIterator createIterator(Node node, DocumentContext context, Map<String, FeatureFactory> ff) throws Exception {
    return createIterator(node, context, ff.get(node.getIndexTarget(default_index)));
  }

  protected StructuredIterator createIterator(Node node, DocumentContext context, FeatureFactory ff) throws Exception {
    HashMap<String, StructuredIterator> iteratorCache = new HashMap();
    return createNodeMergedIterator(node, context, iteratorCache, ff);
  }

  protected StructuredIterator createNodeMergedIterator(Node node, DocumentContext context,
          HashMap<String, StructuredIterator> iteratorCache, FeatureFactory ff)
          throws Exception {
    ArrayList<StructuredIterator> internalIterators = new ArrayList<StructuredIterator>();
    StructuredIterator iterator;

    // first check if the cache contains this node
    if (iteratorCache.containsKey(node.toString())) {
      return iteratorCache.get(node.toString());
    }

    try {
      for (Node internalNode : node.getInternalNodes()) {
        StructuredIterator internalIterator = createNodeMergedIterator(internalNode, context, iteratorCache, ff);
        internalIterators.add(internalIterator);
      }
      /* Again we look at the node (query) for what index is being used. */
      String index_key = node.getIndexTarget(default_index);

      if (!index.containsKey(index_key)) {
        throw new IOException("Index requested (in createNodeMergedIterator) was not found to be loaded!!");
      }

      iterator = index.get(index_key).getIterator(node);
      if (iterator == null) {
        iterator = ff.getIterator(node, internalIterators, this);
      }
    } catch (Exception e) {
      throw e;
    }
    if (ContextualIterator.class.isInstance(iterator) && (context != null)) {
      ((ContextualIterator) iterator).setContext(context);
    }

    // we've created a new iterator - add to the cache for future nodes
    iteratorCache.put(node.toString(), iterator);
    //System.err.printf("node %s = iterator %s\n", node.toString(), iterator.toString());
    return iterator;
  }

  public Node transformBooleanQuery(Node queryTree, String retrievalGroup) throws Exception {
    String index_key = queryTree.getIndexTarget(default_index);
    return transformQuery(booleanFeatureFactory.get(index_key).getTraversals(this), queryTree, retrievalGroup);
  }

  public Node transformCountQuery(Node queryTree, String retrievalGroup) throws Exception {
    String index_key = queryTree.getIndexTarget(default_index);
    return transformQuery(countFeatureFactory.get(index_key).getTraversals(this), queryTree, retrievalGroup);
  }

  public Node transformRankedQuery(Node queryTree, String retrievalGroup) throws Exception {
    String index_key = queryTree.getIndexTarget(default_index);
    return transformQuery(rankedFeatureFactory.get(index_key).getTraversals(this), queryTree, retrievalGroup);
  }

  private Node transformQuery(List<Traversal> traversals, Node queryTree, String retrievalGroup) throws Exception {
    for (Traversal traversal : traversals) {
      queryTree = StructuredQuery.copy(traversal, queryTree);
    }
    return queryTree;
  }

  /**
   * Returns the number of occurrences of the provided
   * expression. If the expression does not produce a CountIterator
   * as a node type, throws an IllegalArgumentException, since it's not
   * an appropriate input. #text, #ow, and #uw should definitely be ok here.
   *
   * @param nodeString
   * @return Number of times the expression occurs.
   * @throws Exception
   */
  public long xCount(String nodeString) throws Exception {

    // first parse the node
    Node root = StructuredQuery.parse(nodeString);
    return xCount(root);
  }

  public long xCount(Node root) throws Exception {

    System.err.printf("Running xcount: %s\n", root.toString());

    NodeCountAggregator agg = new NodeCountAggregator(root);
    return agg.termCount();
  }

  public long docCount(String nodeString) throws Exception {

    // first parse the node
    Node root = StructuredQuery.parse(nodeString);
    return docCount(root);
  }

  public long docCount(Node root) throws Exception {

    System.err.printf("Running doccount: %s\n", root.toString());

    NodeCountAggregator agg = new NodeCountAggregator(root);
    return agg.documentCount();
  }

  public NodeType getNodeType(Node node, String retrievalGroup) throws Exception {
    String index_key = node.getIndexTarget(default_index);
    if (!index.containsKey(index_key)) {
      throw new IOException("Index requested was not found to be loaded!!");
    }

    NodeType nodeType = index.get(index_key).getNodeType(node);
    if (nodeType == null) {
      nodeType = rankedFeatureFactory.get(index_key).getNodeType(node);
    }
    return nodeType;
  }

  /**
   * Subclass that counts the number of occurrences of the provided
   * expression. If the expression does not produce a CountIterator
   * as a node type, throws an IllegalArgumentException, since it's not
   * an appropriate input. #text, #ow, and #uw should be ok here.
   *
   * both term and document counts are maintained.
   *
   * author sjh
   *
   */
  public class NodeCountAggregator {

    private long docCount;
    private long termCount;

    public NodeCountAggregator(Node root) throws IOException, Exception {
      docCount = 0;
      termCount = 0;

      String index_key = root.getIndexTarget(default_index);
      StructuredIterator structIterator = createIterator(root, null, countFeatureFactory.get(index_key));
      if (PositionIndexReader.AggregateIterator.class.isInstance(structIterator)) {
        docCount = ((PositionIndexReader.AggregateIterator) structIterator).totalEntries();
        termCount = ((PositionIndexReader.AggregateIterator) structIterator).totalPositions();
      } else if (structIterator instanceof CountIterator) {
        CountValueIterator iterator = (CountValueIterator) structIterator;
        while (!iterator.isDone()) {
          if (iterator.hasMatch(iterator.currentCandidate())) {
            termCount += iterator.count();
            docCount++;
          }
          iterator.next();
        }
      } else {
        throw new IllegalArgumentException("Node " + root.toString() + " did not return a counting iterator.");
      }
    }

    public long documentCount() throws IOException {
      return docCount;
    }

    public long termCount() throws IOException {
      return termCount;
    }
  }
}
