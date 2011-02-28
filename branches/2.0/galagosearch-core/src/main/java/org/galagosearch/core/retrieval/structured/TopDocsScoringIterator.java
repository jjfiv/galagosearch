/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.structured;

import gnu.trove.TObjectDoubleHashMap;
import java.io.File;
import java.io.IOException;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.index.TopDocsReader;
import org.galagosearch.core.scoring.ScoringFunction;
import org.galagosearch.tupleflow.Parameters;

/**
 * It's a scoring node, but it's not a scoring function node, nor is it really
 * a score combination node. It's more like a scoring shortcut node, since we can
 * interpose between the parent node and the child scorer, and use the topdocs if
 * the parent is aware. Otherwise all methods are passthru.
 *
 * The assumption made here is that if you're using this wrapper, you intend to make use
 * of the topdocs somehow - therefore they are loaded on construction. We do this in order to
 * make the maxscore of this iterator a realistic value. Otherwise it's the theoretical bound.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"term"})
public class TopDocsScoringIterator extends TransformIterator {

  static TopDocsReader reader = null;
  TopDocsReader.ListIterator tdIterator;
  ScoringFunction function;
  double maxscore;

  public TopDocsScoringIterator(Parameters p, ScoringFunctionIterator sfi) {
    super(sfi);
    function = sfi.getScoringFunction();
    maxscore = sfi.maximumScore();
    tdIterator = null;
    if (p.containsKey("index")) {
      try {
        if (reader == null) {
          String path = p.get("index") + File.separator 
            + p.get("loc") + ".topdocs";
          reader = (TopDocsReader) StructuredIndex.openIndexPart(path);
        }

        tdIterator = reader.getTopDocs(p.get("term"));
        
	// Assumes score-sorted order. It's not currently.
	//TopDocument td = tdIterator.getCurrentTopDoc();
        //maxscore = function.score(td.count, td.length);
      } catch (Exception e) {
        tdIterator = null;
      }
    }
  }

  public TopDocsReader.ListIterator getTopDocs() {
    return tdIterator;
  }

  /**
   * The existence of top docs gives us a convenient way to
   * recover an accurate max score for this iterator.
   *
   * @return
   */
  public double maximumScore() {
    return maxscore;
  }

  public double minimumScore() {
    return ((ScoringFunctionIterator)iterator).minimumScore();
  }

  public void setMaximumScore(double newmax) {
    maxscore = newmax;
  }

  public void lowerMaximumScore(double newmax) {
    if (newmax < maxscore) {
      maxscore = newmax;
    }
  }

  @Override
  public double score() {
    return ((ScoringFunctionIterator)iterator).score();
  }

  public double score(DocumentContext dc) {
    return ((ScoringFunctionIterator)iterator).score(dc);
  }

  public ScoringFunction getScoringFunction() {
    return function;
  }

  public TObjectDoubleHashMap<String> parameterSweepScore() {
    return ((ScoringFunctionIterator)iterator).parameterSweepScore();
  }
}
