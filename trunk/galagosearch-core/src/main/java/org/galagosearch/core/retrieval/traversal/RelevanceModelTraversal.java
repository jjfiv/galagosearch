package org.galagosearch.core.retrieval.traversal;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.galagosearch.core.parse.CorpusReader;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.parse.TagTokenizer;
import org.galagosearch.core.index.DocumentLengthsReader;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.core.util.TextPartAssigner;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 * The Relevance Model implemented as a traversal. Query should look like:
 *
 *  #rm:fbOrigWt=0.5:fbDocs=10:fbTerms=10( query )
 *
 * The outer node (the #rm operator) will be replaced with a #combine, and the 
 * query submitted to the retrieval supplied at construction. The parameters
 * will then be applied for constructing the expansion.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"retrievalGroup", "index"})
public class RelevanceModelTraversal implements Traversal {
    Retrieval retrieval;
    Parameters queryParameters;
    Parameters availableParts;
    TagTokenizer tokenizer;

  public RelevanceModelTraversal(Parameters parameters, Retrieval retrieval) throws IOException {
      this.retrieval = retrieval;
      this.queryParameters = parameters;
      this.availableParts = retrieval.getAvailiableParts(parameters.get("retrievalGroup"));
      tokenizer = new TagTokenizer();
  }

  public Node afterNode(Node newNode) throws Exception {
    if (newNode.getOperator().equals("rm") == false) return newNode;

    // Kick off the inner query
    Parameters parameters = newNode.getParameters();
    int fbDocs = (int) parameters.get("fbDocs", 10);
    Node combineNode = new Node("combine", newNode.getInternalNodes());
    ArrayList<ScoredDocument> initialResults = new ArrayList<ScoredDocument>();

    // Only get as many as we need
    Parameters localParameters = queryParameters.clone();
    localParameters.set("requested", Integer.toString(fbDocs));

    retrieval.runAsynchronousQuery(combineNode, localParameters, initialResults);
    
    // while that's running, extract the feedback parameters
    double fbOrigWt = parameters.get("fbOrigWt", 0.5);

    int fbTerms = (int) parameters.get("fbTerms", 10);

    // Let's also make a corpus reader
    String corpusLocation = queryParameters.get("corpus", null);
    if (corpusLocation == null) { // keep trying
      corpusLocation = queryParameters.get("index") + File.separator + "corpus";
    }
    CorpusReader cReader = new CorpusReader(corpusLocation);
       
    // We also need the document lengths
    DocumentLengthsReader docLengths = new DocumentLengthsReader(queryParameters.get("index") 
								 + File.separator + "documentLengths");
    
    // Now we wait
    retrieval.waitForAsynchronousQuery();

    // No sort needed - would've been done by the retrieval
    HashMap<Integer, Double> scores = logstoposteriors(initialResults);
    HashMap<String, HashMap<Integer,Integer>> counts = countGrams(initialResults, cReader);
    cReader.close();

    // now score
    ArrayList<Gram> scored = scoreGrams(counts, scores, docLengths);
    Collections.sort(scored);
    Node newRoot = null;
    docLengths.close();

    // Time to construct the modified query - start with the expansion since we always have it
    // make sure we filter stopwords
    HashSet<String> stopwords = Utility.readStreamToStringSet(getClass().getResourceAsStream("/stopwords/inquery"));
    Set<String> queryTerms = StructuredQuery.findQueryTerms(combineNode, "extents");
    stopwords.addAll(queryTerms);
    Parameters expParams = new Parameters();
    ArrayList<Node> newChildren = new ArrayList<Node>();
    int expanded = 0;
    for (int i=0; i < scored.size() && expanded < fbTerms; i++) {
      Gram g = scored.get(i);
      if (stopwords.contains(g.term)) continue;
      Node inner = TextPartAssigner.assignPart(new Node("text", g.term), availableParts);
      ArrayList<Node> innerChild = new ArrayList<Node>();
      innerChild.add(inner);
      newChildren.add(new Node("feature", "dirichlet", innerChild));
      expParams.set(Integer.toString(expanded), Double.toString(g.score));
      expanded++;
    }
    Node expansionNode = new Node("combine", expParams, newChildren, 0);

    if (fbOrigWt == 0.0) {
      expansionNode = newRoot;
    } else {
      expParams = new Parameters();
      expParams.set("1", Double.toString(fbOrigWt));
      expParams.set("2", Double.toString(1-fbOrigWt));
      newChildren = new ArrayList<Node>();
      newChildren.add(combineNode);
      newChildren.add(expansionNode);
      newRoot = new Node("combine", expParams, newChildren, 0);
    }
    return newRoot;
  }

  public void beforeNode(Node object) throws Exception {
  }

  // Implementation here is identical to the Relevance Model unigram normaliztion in Indri.
  // See RelevanceModel.cpp for details
  protected HashMap<Integer, Double> logstoposteriors(ArrayList<ScoredDocument> results) {
    HashMap<Integer, Double> scores = new HashMap<Integer, Double>();
    if (results.size() == 0) return scores;

    // For normalization
    double K = results.get(0).score;

    // First pass to get the sum
    double sum = 0;
    for (ScoredDocument sd : results) {
      sd.score = Math.exp(K+sd.score);
      sum += sd.score;
    }

    // Normalize
    for (ScoredDocument sd : results) {
      sd.score /= sum;
      scores.put(sd.document, sd.score);
     }
    return scores;
  }

  protected HashMap<String, HashMap<Integer, Integer>> countGrams(ArrayList<ScoredDocument> results, CorpusReader reader) throws IOException {
    HashMap<String, HashMap<Integer, Integer>> counts = new HashMap<String, HashMap<Integer,Integer>>();
    HashMap<Integer, Integer> termCounts;
    Document doc;
    for (ScoredDocument sd : results) {
      doc = reader.getDocument(sd.documentName);
      tokenizer.tokenize(doc);
      for (String term : doc.terms) {
        if (!counts.containsKey(term)) counts.put(term, new HashMap<Integer, Integer>());
        termCounts = counts.get(term);
        if (termCounts.containsKey(sd.document)) termCounts.put(sd.document, termCounts.get(sd.document)+1);
        else termCounts.put(sd.document, 1);
      }
    }
    return counts;
  }

    protected ArrayList<Gram> scoreGrams(HashMap<String, HashMap<Integer, Integer>> counts, HashMap<Integer, Double> scores, DocumentLengthsReader docLengths)
    throws Exception {
    ArrayList<Gram> grams = new ArrayList<Gram>();
    HashMap<Integer, Integer> termCounts;
    HashMap<Integer, Integer> lengthCache = new HashMap<Integer, Integer>();

    for (String term : counts.keySet()) {
      Gram g = new Gram(term);
      termCounts = counts.get(term);
      for (Integer docID : termCounts.keySet()) {
	  if (!lengthCache.containsKey(docID)) {
	      lengthCache.put(docID, docLengths.getLength(docID));
	  }
	  int length = lengthCache.get(docID);
        g.score += scores.get(docID) * termCounts.get(docID) / length;
      }
      // 1 / fbDocs from the RelevanceModel source code
      g.score *= (1.0 / scores.size());
      grams.add(g);
    }

    return grams;
  }
  
  public static class Gram implements Comparable<Gram> {
    public String term;
    public double score;

    public Gram(String t) {
      term = t;
      score = 0.0;
    }

    // The secondary sort is to have defined behavior for statistically tied samples.
    public int compareTo(Gram that) {
      int result =  this.score > that.score ? -1 : (this.score < that.score ? 1 : 0);
      if (result != 0) return result;
      result = (this.term.compareTo(that.term));
      return result;
    }

    public String toString() {
        return "<" + term + "," + score + ">";
    }
  }
}
