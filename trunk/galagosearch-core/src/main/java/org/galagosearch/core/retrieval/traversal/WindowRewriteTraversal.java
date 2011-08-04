// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.traversal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.galagosearch.core.parse.Porter2Stemmer;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.tupleflow.Parameters;

/*
 * window operator
 * 
 * rewrites the window meta-operator :
 *  #window:part=n3-w1-ordered-h2( term term term )
 *   or
 *  #window:width=1:ordered=true:h=2:docfreq=false:stemmed=false( term term term )
 * 
 * to an operator that can be run on an n-gram index :
 * 
 *  #extent:part=n3-w2-ordered-h2:term~term~term()
 * 
 * @author sjh
 * 
 */
@RequiredStatistics(statistics = {"retrievalGroup"})
public class WindowRewriteTraversal implements Traversal {

  Parameters availiableParts;
  Porter2Stemmer stemmer;

  public WindowRewriteTraversal(Parameters parameters, Retrieval retrieval) throws IOException {
    this.availiableParts = retrieval.getAvailableParts(parameters.get("retrievalGroup"));
    stemmer = new Porter2Stemmer();
  }

  /*
   * before node checks that an ngram operator is possible
   * 
   */
  public void beforeNode(Node node) throws Exception {
    if (node.getOperator().equals("window")) {
      for (Node child : node.getInternalNodes()) {
        if (!child.getOperator().equals("text")) {
          throw new Exception("Arguments of an window operator need to be text nodes.\n"
                  + "Problem argument : \n" + child.toString());
        }
      }

      int n = node.getInternalNodes().size();
      if (!possibleIndexExists(n)) {
        throw new Exception("No indexes found for " + n + "-windows. Aborting.\n" + node.toString());
      }
    }
  }

  /*
   * after node creates the extent node that accesses the correct ngram index
   * 
   */
  public Node afterNode(Node original) throws Exception {

    if (original.getOperator().equals("window")) {
      Parameters p = original.getParameters();
      if (!p.containsKey("part")) {
        int n = original.getInternalNodes().size();
        String part = getnGramPartName(n, p);
        p.add("part", part);
      }

      ArrayList<Node> children = original.getInternalNodes();
      StringBuilder sb = new StringBuilder();
      sb.append(children.get(0).getDefaultParameter());

      for (int i = 1; i < children.size(); i++) {
        sb.append("~").append(children.get(i).getDefaultParameter());
      }
      p.set("default", sb.toString());
      return new Node("extents", p, new ArrayList<Node>(), original.getPosition());
    }
    return original;
  }

  // minimum work to ensure that an n-gram index exists
  private boolean possibleIndexExists(int n) {
    for (String part : availiableParts.stringList("part")) {
      if (part.startsWith("n" + Integer.toString(n))) {
        return true;
      }
    }
    return false;
  }

  /* pick index part matching the parameters
   * 
   *  window indexes look like: nX-wX-[un]ordered-hX[-docfreq][-stemmed]
   */
  private String getnGramPartName(int n, Parameters p) {
    String selectedPart = null;
    int currentThreshold = Integer.MAX_VALUE;
    

    // first build a set of requirements
    HashSet<String> desiredPartAttributes = new HashSet();
    desiredPartAttributes.add("n" + Integer.toString(n));
    if (p.containsKey("width")) {
      desiredPartAttributes.add("-w" + p.get("width"));
    }
    if (p.containsKey("threshold")) {
      desiredPartAttributes.add("-h" + p.get("threshold"));
    }
    if (p.containsKey("ordered")) {
      if (Boolean.parseBoolean(p.get("ordered"))) {
        desiredPartAttributes.add("-ordered");
      } else {
        desiredPartAttributes.add("-unordered");
      }
    }
    if (p.containsKey("stemming")) {
      if (Boolean.parseBoolean(p.get("stemming"))) {
        desiredPartAttributes.add("-stemmed");
      }
    }
    if (p.containsKey("usedocfreq")) {
      if (Boolean.parseBoolean(p.get("usedocfreq"))) {
        desiredPartAttributes.add("-docfreq");
      }
    }

    List<String> parts = availiableParts.stringList("part");
    Collections.sort(parts);
    for (String part : parts) {
      boolean flag = true;
      for(String attr : desiredPartAttributes){
        if(!part.contains(attr)){
          flag = false;
        }
      }
      // if all attributes match
      if(flag){
        String hValue = part.split("-")[3].replace("h","");
        int h = Integer.parseInt(hValue);
        if(h <= currentThreshold){
          currentThreshold = h;
          selectedPart = part;
        }
      }
    }
    return selectedPart;
  }
}
