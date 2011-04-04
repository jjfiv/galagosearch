// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import gnu.trove.TIntArrayList;
import gnu.trove.TObjectIntHashMap;
import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntHashSet;
import gnu.trove.TObjectDoubleHashMap;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import org.galagosearch.core.index.DocumentLengthsReader;
import org.galagosearch.core.index.TopDocsReader;
import org.galagosearch.core.index.TopDocsReader.TopDocument;
import org.galagosearch.core.scoring.ScoringFunction;
import org.galagosearch.core.util.CallTable;
import org.galagosearch.tupleflow.Parameters;

/**
 * Galago's implementation of the max_score algorithm for document-at-a-time processing.
 * The implementation is as follows:
 *
 * Init) Scan children for topdocs. If they exist, iterate over those first to bootstrap bounds.
 *       Otherwise while the number of scored docs from this iterator is under requested, make no
 *       adjustments.
 *
 * For each candidate document:
 *
 * If scored is less than requested, fully score the document, add it to the best seen queue, and return
 * score.
 * Otherwise,
 * While we have not found a candidate with a score above max_score:
 *  For all iterators:
 *      1) Choose the iterator with the shortest estimated list of candidates
 *      2) Determine if the score + remaining upper bounds is less than max_score. If so, stop scoring and move to the next candidate.
 *         Else move to next iterator.
 *
 *      3) If all iterators are used, mark that document as the scored document, and update max_score if needed.
 *
 * In order to utilize the optimization properly, we need to lazy-move the iterators involved in scoring.
 * Therefore, moveTo and movePast will only set the "nextDocumentToScore" variable to indicate where the first
 * iterator should move to if we're required to score.
 *
 * Things are a bit trickier when information is requested of the iterator. The semantics are:
 *  hasMatch(doc): Does the iterator have 'doc' in its candidate list? In order to answer this accurately, we need to
 *                 attempt to score. Given that this is usually called after a move call, we score forward until we reach
 *                 nextDocumentToScore. If lastDocumentScored == doc, return true, meaning it made the cut.
 *  currentCandidate(): What is the next candidate according to this iterator? Once again, we need to attempt to score.
 *                 however in this case we need to continue iterating until we find a winner. There is no external bound.
 *  score(doc, length): We need to score. If doc == 		   
 * @author irmarc
 */
@RequiredStatistics(statistics = {"index"})
public class MaxScoreCombinationIterator extends ScoreCombinationIterator {

    private class Placeholder implements Comparable<Placeholder> {
	
	int document;
	double score;
	
	public Placeholder() {
	}
	
	public Placeholder(int d, double s) {
	    document = d;
	    score = s;
	}
	
	public int compareTo(Placeholder that) {
	    return this.score > that.score ? -1 : this.score < that.score ? 1 : 0;
	}
	
	public String toString() {
	    return String.format("<%d,%f>", document, score);
	}
    }    

  private static class DocumentOrderComparator implements Comparator<DocumentOrderedScoreIterator> {

    public int compare(DocumentOrderedScoreIterator a, DocumentOrderedScoreIterator b) {
      return (a.currentCandidate() - b.currentCandidate());
    }
  }

  private class WeightComparator implements Comparator<DocumentOrderedScoreIterator> {
      public int compare(DocumentOrderedScoreIterator a, DocumentOrderedScoreIterator b) {
	  if (!a.isDone() && b.isDone()) return -1;
	  if (a.isDone() && !b.isDone()) return 1;
	  double w1 = weightLookup.get(a);
	  double w2 = weightLookup.get(b);
	  return (w1 > w2 ? -1 : (w1 < w2 ? 1 : 0));
      }
  }

  private static class TotalCandidateComparator implements Comparator<DocumentOrderedScoreIterator> {

    public int compare(DocumentOrderedScoreIterator a, DocumentOrderedScoreIterator b) {
	if (!a.isDone() && b.isDone()) return -1;
	if (a.isDone() && !b.isDone()) return 1;
	return (a.totalCandidates() < b.totalCandidates() ? -1 : a.totalCandidates() > b.totalCandidates() ? 1 : 0);
    }
  }
  int requested;
  double threshold = 0;
  double potential;
  double minimum;
  TObjectDoubleHashMap weightLookup;
  TIntArrayList topdocsCandidates;
  int lastReportedCandidate = 0;
  int candidatesIndex;
  int quorumIndex;
  ArrayList<DocumentOrderedScoreIterator> scoreList;
  ArrayList<Placeholder> R;
    TIntHashSet ids;

  public MaxScoreCombinationIterator(Parameters parameters,
          DocumentOrderedScoreIterator[] childIterators) throws IOException {
    super(parameters, childIterators);
    requested = (int) parameters.get("requested", 100);
    R = new ArrayList<Placeholder>();
    ids = new TIntHashSet();
    topdocsCandidates = new TIntArrayList();
    scoreList = new ArrayList<DocumentOrderedScoreIterator>(iterators.length);   	
    weightLookup = new TObjectDoubleHashMap();
    ArrayList<DocumentOrderedScoreIterator> topdocs = new ArrayList<DocumentOrderedScoreIterator>();
    for (int i = 0; i < iterators.length; i++) {
      DocumentOrderedScoreIterator dosi = iterators[i];
      weightLookup.put(dosi, weights[i]);
      if (TopDocsScoringIterator.class.isAssignableFrom(dosi.getClass())) {
        topdocs.add(dosi);
      }
    }
    cacheScores(topdocs);
    scoreList.addAll(Arrays.asList(iterators));
    String sort = parameters.get("sort", "length");
    if (sort.equals("weight")) {
	Collections.sort(scoreList, new WeightComparator());
    } else {
	Collections.sort(scoreList, new TotalCandidateComparator());
    }
    for (int i = 0; i < iterators.length; i++) {
      DocumentOrderedScoreIterator dosi = iterators[i];
      potential += weights[i] * dosi.maximumScore();
      minimum += weights[i] * dosi.minimumScore();
    }
    computeQuorum();
  }
    
    @Override
    public double maximumScore() {
	double s = 0;
	for (DocumentOrderedScoreIterator it : iterators) {
	    s = Math.max(it.maximumScore(), s);
	}
	if (s == Double.MAX_VALUE || s == Double.POSITIVE_INFINITY) return s;

	// Not infinity/max
	s = 0;
	for (int i = 0; i < iterators.length; i++) {
	    s += weights[i] * iterators[i].maximumScore();
	}
	return s / weightSum;
    }


    @Override
    public double minimumScore() {
	double s = 0;
	for (int i = 0; i < iterators.length; i++) {
	    s += weights[i] * iterators[i].minimumScore();
	}
	return s / weightSum;
    }
   
  /**
   * Resets all iterators, does not perform caching again
   * @throws IOException
   */
   @Override
   public void reset() throws IOException {
    for (DocumentOrderedScoreIterator dosi : iterators) {
      dosi.reset();
    }
    candidatesIndex = 0;
    computeQuorum();
  }

  /**
   * Done means
   * 1) No more open iterators, or
   * 2) if scored > requested, none of the open iterators can contribute
   *      enough to get over the limit we've seen so far
   * @return
   */
  @Override
  public boolean isDone() {
      return (currentCandidate() == Integer.MAX_VALUE);
  }

  @Override
  public int currentCandidate() {
      int candidate = Integer.MAX_VALUE;

      // first check the topdocs
      if (candidatesIndex < topdocsCandidates.size()) {
	  candidate = topdocsCandidates.get(candidatesIndex);
      }
      
      // Now look among the quorum iterators
      for (int i = 0; i < quorumIndex; i++) {
	  candidate = Math.min(candidate, scoreList.get(i).currentCandidate());
      }
      lastReportedCandidate = candidate;
      return candidate;
  }

  @Override
  public boolean hasMatch(int document) {
      return (currentCandidate() == document);
  }

  @Override
  public void movePast(int document) throws IOException {
      skipToDocument(document+1);
  }

  @Override
  public void moveTo(int document) throws IOException {
      skipToDocument(document);      
  }



  @Override
  public boolean skipToDocument(int document) throws IOException {
      // Move topdocs candidate list forward
      while (candidatesIndex < topdocsCandidates.size() &&
	     document > topdocsCandidates.get(candidatesIndex)) {
	  candidatesIndex++;
      }

      // Now only skip the quorum forward
      for (int i = 0; i < quorumIndex; i++) {
	  scoreList.get(i).skipToDocument(document);
      }

      return hasMatch(document);
  }

  /**
   * Just sets context for scoring
   */
  public double score() {
    return score(documentToScore, lengthOfDocumentToScore);
  }

  public double score(int document, int length) {
      try {
	  if ((candidatesIndex < topdocsCandidates.size() &&
	       document == topdocsCandidates.get(candidatesIndex)) ||
	      (R.size() < requested)) {
	      // Make sure the iterators are lined up
	      for (DocumentOrderedScoreIterator it : iterators) {
		  it.skipToDocument(lastReportedCandidate);
	      }
	      double score = fullyScore(document, length);
	      adjustThreshold(document, score);
	      return score / weightSum;
	  } else {
	      // First score the quorum, then as we score the rest, skip it forward.	  
	      double adjustedPotential = potential;
	      double inc;
	      int i;
	      for (i = 0; i < quorumIndex; i++) {
		  DocumentOrderedScoreIterator it = scoreList.get(i);
		  inc = weightLookup.get(it) * (it.score(document, length) - it.maximumScore());
		  adjustedPotential += inc;
	      }

	      while (adjustedPotential > threshold && i < scoreList.size()) {
		  DocumentOrderedScoreIterator it = scoreList.get(i);
		  it.skipToDocument(lastReportedCandidate);
		  adjustedPotential += weightLookup.get(it) * (it.score(document, length) - it.maximumScore());
		  i++;
	      }
	      
	      // fully scored!
	      if (i == scoreList.size()) {
		  adjustThreshold(document, adjustedPotential);
		  return adjustedPotential / weightSum; 
	      } else {
		  // didn't make the cut
		  CallTable.increment("iterator_pruned", (scoreList.size()-i));
		  return minimum / weightSum;
	      }
	  }
      } catch (IOException ioe) {
	  throw new RuntimeException(ioe);
      }	  
  }
    
    private double fullyScore(int document, int length) {
	double total = 0;	  
	for (int i = 0; i < iterators.length; i++) {
	    total += weights[i] * iterators[i].score(document, length);
	}
	return total;
    }

  /**
   * Determines if the top candidates list has changed. 
   * Note that R is just a queue of doubles - if you add
   * a score of a single doc multiple times, it won't know any better.
   * @param newscore
   */
  protected void adjustThreshold(int document, double newscore) {
      if (ids.contains(document)) {
	  for (Placeholder p : R) {
	      if (p.document == document) {
		  p.score = newscore;
		  break;
	      }
	  }
      } else {
	  if (R.size() < requested) {
	      R.add(new Placeholder(document, newscore));	      
	      ids.add(document);
	  } else {
	      if (newscore > R.get(requested-1).score) {
		  Placeholder old = R.remove(requested-1);
		  R.add(new Placeholder(document, newscore));
		  ids.remove(old.document);
		  ids.add(document);
	      }
	  }
      }
      if (R.size() == requested) {
	  Collections.sort(R);
	  threshold = R.get(requested-1).score;
	  computeQuorum();
      }
  }

    /**
     * Determines which iterators we need to worry about to produce a valid
     * candidate score. If scored < requested, it defaults to all of them.
     */
  protected void computeQuorum() {
      double adjustedPotential = 0;
      if (R.size() < requested) {
	  quorumIndex = scoreList.size();
      } else {
	  // Now we try
	  adjustedPotential = potential;
	  int i;
	  for (i = 0; i < scoreList.size() && adjustedPotential > threshold; i++) {
	      DocumentOrderedScoreIterator it = scoreList.get(i);
	      double inc = weightLookup.get(it) * (it.minimumScore() - it.maximumScore());
		      adjustedPotential += inc;
	  }
	  quorumIndex = i;	  
      }
  }
  /**
   * We pre-evaluate the the topdocs lists by scoring them against all iterators
   * that have topdocs, giving us a partial evaluation, and therefore a better
   * threshold bound - we also move the scored count up in order to activate the
   * threshold sooner.
   */
  protected void cacheScores(ArrayList<DocumentOrderedScoreIterator> topdocs) throws IOException {
    // Iterate over the lists, scoring as we go - they are doc-ordered after all
    TObjectIntHashMap loweredCount = new TObjectIntHashMap();
    PriorityQueue<TopDocsReader.Iterator> toScore = new PriorityQueue<TopDocsReader.Iterator>();
    HashMap<TopDocsReader.Iterator, TopDocsScoringIterator> lookup =
            new HashMap<TopDocsReader.Iterator, TopDocsScoringIterator>();
    TIntDoubleHashMap scores = new TIntDoubleHashMap();
    for (DocumentOrderedScoreIterator dosi : topdocs) {
      TopDocsScoringIterator tdsi = (TopDocsScoringIterator) dosi;
      TopDocsReader.Iterator it = tdsi.getTopDocs();
      if (it != null) {
	  toScore.add(it);
	  lookup.put(it, tdsi);
      }
    }

    if (toScore.size() == 0) return;

    double score;

    // Now we can simply iterate until done
    while (!toScore.peek().isDone()) {
      TopDocsReader.Iterator it = toScore.poll();
      TopDocument td = it.getCurrentTopDoc();

      // Need the background score first
      score = 0;
      for (DocumentOrderedScoreIterator dosi: iterators) {
	  score += weightLookup.get(dosi) * dosi.score(0, td.length);
      }

      scores.put(td.document, score);
      // Score it using this iterator
      ScoringFunction fn = lookup.get(it).getScoringFunction();
      score = weightLookup.get(lookup.get(it))
	  * (fn.score(td.count, td.length) - fn.score(0, td.length));
      scores.adjustValue(td.document, score);
      lookup.get(it).lowerMaximumScore(score);
      it.movePast(td.document);
      // Now score w/ the others
      for (TopDocsReader.Iterator tdrit : toScore) {
        if (tdrit.hasMatch(td.document)) {
          TopDocument other = tdrit.getCurrentTopDoc();
	  fn = lookup.get(tdrit).getScoringFunction();
          score = weightLookup.get(lookup.get(tdrit))
	      * (fn.score(other.count, other.length) - fn.score(0, other.length));
          scores.adjustValue(other.document, score);
	  lookup.get(tdrit).lowerMaximumScore(score);
          tdrit.movePast(other.document); // scored - move on
        }
      }
      toScore.add(it); // put it back in, do it again
      topdocsCandidates.add(td.document);
    }

    double[] values = scores.getValues();
    int[] keys = scores.keys();
    for (int i = 0; i < keys.length; i++) {	
	R.add(new Placeholder(keys[i], scores.get(keys[i])));
    }
    Collections.sort(R);
    while (R.size() > requested) R.remove(R.size()-1);
    threshold = R.get(R.size()-1).score;
    R.trimToSize();
    for (int i = 0; i < R.size(); i++) {
	ids.add(R.get(i).document);
    }
    candidatesIndex = 0;
  }
}
