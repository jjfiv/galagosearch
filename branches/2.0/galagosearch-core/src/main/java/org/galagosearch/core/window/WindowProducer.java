// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.window;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.galagosearch.core.parse.NumberedDocument;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * <p> Produces windows consisting of n words. </p>
 *
 * <p> Windows
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.parse.NumberedDocument")
@OutputClass(className = "org.galagosearch.core.window.Window")
public class WindowProducer extends StandardStep<NumberedDocument, Window> {

  int n;
  int width;
  boolean ordered;
  LinkedList<String> window;
  int currentDocument;
  int currentBegin;

  public WindowProducer(TupleFlowParameters parameters) throws IOException {
    this.n = Integer.parseInt(parameters.getXML().get("n"));
    this.width = Integer.parseInt(parameters.getXML().get("width"));
    this.ordered = Boolean.parseBoolean(parameters.getXML().get("ordered"));
  }

  /*
   * n-gram format:
   *   "w1~w2~w3"
   * 
   */
  public void process(NumberedDocument doc) throws IOException {

    //if this document is not big enough to contain any ngrams
    if (doc.terms.size() < n) {
      return;
    }

    window = new LinkedList();
    currentDocument = doc.number;
    currentBegin = 0;

    if (ordered) {
      for (currentBegin = 0; currentBegin < doc.terms.size(); currentBegin++) {
        window.push(doc.terms.get(currentBegin));
        extractOrderedWindows(doc.terms, currentBegin);
        window.pop();
      }
    } else {
      for (currentBegin = 0; currentBegin < doc.terms.size(); currentBegin++) {
        window.push(doc.terms.get(currentBegin));
        extractUnorderedWindows(doc.terms, currentBegin);
        window.pop();
      }
    }
  }

  private void extractOrderedWindows(List<String> terms, int currentEnd) throws IOException {
    // print(window);
    if (window.size() == n) {
      processor.process(new Window(currentDocument, currentBegin, currentEnd, ConvertToBytes(window)));
    } else {
      // System.err.println("loop = " + (currentEnd+1) + "\t" + (currentEnd + this.width));
      for (int i = currentEnd + 1; i < (currentEnd + this.width + 1); i++) {
        if( i < terms.size() ){
          window.push(terms.get(i));
          extractOrderedWindows(terms, i);
          window.pop();
        }
      }
    }
  }

  private void extractUnorderedWindows(List<String> terms, int currentEnd) throws IOException {
    if (window.size() == n) {
      LinkedList<String> sortedWindow = new LinkedList(window);
      Collections.sort(sortedWindow, Collections.reverseOrder());
      processor.process(new Window(currentDocument, currentBegin, currentEnd, ConvertToBytes(sortedWindow)));
    } else {
      for (int i = currentEnd + 1; i < currentBegin + width; i++) {
        if( i < terms.size() ){
          window.push(terms.get(i));
          extractUnorderedWindows(terms, i);
          window.pop();
        }
      }
    }
  }

  private static byte[] ConvertToBytes(List<String> windowData) {
    StringBuilder sb = new StringBuilder();
    sb.append(windowData.get( windowData.size() - 1 ));
    for (int i = (windowData.size()-2); i >= 0; i--) {
      sb.append("~").append(windowData.get(i));
    }
    return Utility.fromString(sb.toString());
  }

  private void print(LinkedList<String> window) {
    for(String w : window){
      System.err.print(w + " ");
    }
    System.err.println();
  }
}
