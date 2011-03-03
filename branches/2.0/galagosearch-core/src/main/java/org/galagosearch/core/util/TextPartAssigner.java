/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.util;
 
import java.util.List;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.tupleflow.Parameters;
import org.tartarus.snowball.ext.englishStemmer;

/**
 *
 * @author irmarc
 */
public class TextPartAssigner {

  private static englishStemmer stemmer;

  public static Node assignPart(Node original, Parameters parts) {
      List<String> available = parts.stringList("part");
    if (available.contains("stemmedPostings")) {
	return stemmedNode(original, "stemmedPostings", "extents");
    } else if (available.contains("postings")) {
      return transformedNode(original, "extents", "postings");
    } else {
      return original;
    }
  }

  public static Node transformedNode(Node original,
          String operatorName, String indexName) {
    Parameters parameters = original.getParameters().clone();
    parameters.add("part", indexName);
    return new Node(operatorName, parameters, original.getInternalNodes(), original.getPosition());
  }

    public static Node stemmedNode(Node original, String indexName, String operatorName) {
    Parameters parameters = original.getParameters().clone();
    parameters.add("part", indexName);
    String term = parameters.get("default");
    stemmer.setCurrent(term);
    stemmer.stem();
    String stemmed = stemmer.getCurrent();
    parameters.set("default", stemmed);
    return new Node(operatorName, parameters, original.getInternalNodes(), original.getPosition());
  }

  static {
    stemmer = new englishStemmer();
  }
}
