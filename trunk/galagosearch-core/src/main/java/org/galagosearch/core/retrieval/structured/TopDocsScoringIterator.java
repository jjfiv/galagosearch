/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.structured;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.index.TopDocsReader;
import org.galagosearch.core.index.TopDocsReader.TopDocument;
import org.galagosearch.core.scoring.ScoringFunction;
import org.galagosearch.tupleflow.Parameters;

/**
 * It's a scoring node, but it's not a scoring function node, nor is it really
 * a score combination node. It's more like a scoring shortcut node, since we can
 * interpose between the parent node and the child scorer, and use the topdocs if
 * the parent is aware. Otherwise all methods are passthru.
 *
 * The assumption made here is that if you're using this wrapper, you intend to make use
 * of the topdocs somehow - therefore they are loaded on construction. The other option would
 * be to load them on command, but whatever for now.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics={"index","part","term"})
public class TopDocsScoringIterator extends DocumentOrderedScoreIterator {
    ScoringFunctionIterator mainIterator;
    TopDocsReader.Iterator tdIterator;
    ScoringFunction function;
    double maxscore = Double.MAX_VALUE;

    public TopDocsScoringIterator(Parameters p, ScoringFunctionIterator sfi) {
        mainIterator = sfi;
        function = sfi.getScoringFunction();
        String path = p.get("index") + File.separator + "parts" +
                p.get("part") + ".topdocs";
        try {
            TopDocsReader tdReader = (TopDocsReader) StructuredIndex.openIndexPart(path);
            if (tdReader != null) {
               tdIterator = tdReader.getTopDocs(p.get("term"));
               TopDocument td = tdIterator.getCurrentTopDoc();
               maxscore = function.score(td.count, td.length);
            } else {
               tdIterator = null;
            }
        } catch (Exception e) {
            tdIterator = null;
        }
    }

    public TopDocsReader.Iterator getTopDocs() {
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
        return Double.MIN_VALUE;
    }

    public void setMaximumScore(double newmax) {
        maxscore = newmax;
    }

    public void lowerMaximumScore(double newmax) {
        if (newmax < maxscore) maxscore = newmax;
    }

    @Override
    public boolean isDone() {
        return mainIterator.isDone();
    }

    @Override
    public int currentCandidate() {
        return mainIterator.currentCandidate();
    }

    @Override
    public boolean hasMatch(int document) {
        return mainIterator.hasMatch(document);
    }

    @Override
    public void moveTo(int document) throws IOException {
        mainIterator.moveTo(document);
    }

    @Override
    public void movePast(int document) throws IOException {
        mainIterator.movePast(document);
    }

    @Override
    public boolean skipToDocument(int document) throws IOException {
        return mainIterator.skipToDocument(document);
    }

    @Override
    public void reset() throws IOException {
        mainIterator.reset();
    }

    @Override
    public double score() {
        return mainIterator.score();
    }

    public double score(int document, int length) {
        return mainIterator.score(document, length);
    }

    public ScoringFunction getScoringFunction() {
        return function;
    }

    public Map<String, Double> parameterSweepScore() {
        return mainIterator.parameterSweepScore();
    }
}
