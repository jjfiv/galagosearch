/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.structured;

import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntHashSet;
import gnu.trove.TObjectDoubleHashMap;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import org.galagosearch.core.index.DocumentLengthsReader;
import org.galagosearch.core.index.TopDocsReader;
import org.galagosearch.core.index.TopDocsReader.TopDocument;
import org.galagosearch.tupleflow.Parameters;

/**
 * Galago's implementation of the max_score algorithm for document-at-a-time processing.
 * This current implementation unfortunately does not make use of the "shortest list first"
 * paradigm, as current the children DocumentOrderedScoreIterators aren't required to report
 * length.
 *
 * The implementation is as follows:
 *
 * Init) Scan children for topdocs. If they exist, iterate over those first to bootstrap bounds.
 *       otherwise while the number of scored docs from this iterator is under requested, make no
 *       adjustments.
 *
 * For each candidate document:
 *  For all iterators:
 *      1) Choose the iterator with the highest estimated max scores, and score the candidate.
 *      2) Determine if the score + remaining upper bounds &#060; max_score. If so (and we've seen
 *          at least the requested number of docs), stop scoring and move on. Else, continue.
 *      3) When all iterators have been used, if the score of the document is higher than the
 *          current max_score, then set max_score to that value.
 *
 *
 *
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"index"})
public class MaxScoreCombinationIterator extends ScoreCombinationIterator {

    private static class Placeholder implements Comparable<Placeholder> {

        int document;
        double score;

        public int compareTo(Placeholder that) {
            return (this.document - that.document);
        }
    }

    private static class DocumentOrderComparator implements Comparator<DocumentOrderedScoreIterator> {

        public int compare(DocumentOrderedScoreIterator a, DocumentOrderedScoreIterator b) {
            return (a.currentCandidate() - b.currentCandidate());
        }
    }

    private static class ScoreOrderComparator implements Comparator<DocumentOrderedScoreIterator> {

        public int compare(DocumentOrderedScoreIterator a, DocumentOrderedScoreIterator b) {
            return (a.maximumScore() > b.maximumScore() ? -1
                    : (a.maximumScore() < b.maximumScore() ? 1 : 0));
        }
    }
    int requested;
    int scored;
    double max_score = 0;
    double potential;
    double minimum;
    double currentScore;
    int currentDocument;
    TObjectDoubleHashMap weightLookup;
    TIntHashSet topdocsCandidates;
    PriorityQueue<DocumentOrderedScoreIterator> topdocs;
    PriorityQueue<DocumentOrderedScoreIterator> normal;
    PriorityQueue<Placeholder> cache;
    PriorityQueue<DocumentOrderedScoreIterator> scoreQueue;
    NumberedDocumentDataIterator docLengths;

    public MaxScoreCombinationIterator(Parameters parameters,
            DocumentOrderedScoreIterator[] childIterators) throws IOException {
        super(parameters, childIterators);
        String location = parameters.get("index") + File.separator + "documentLengths";
        DocumentLengthsReader reader = new DocumentLengthsReader(location);
        docLengths = reader.getIterator();
        requested = (int) parameters.get("requested", 100);
        topdocs = new PriorityQueue<DocumentOrderedScoreIterator>(5, new DocumentOrderComparator());
        normal = new PriorityQueue<DocumentOrderedScoreIterator>(5, new DocumentOrderComparator());
        cache = new PriorityQueue<Placeholder>();
        topdocsCandidates = new TIntHashSet();
        scoreQueue = new PriorityQueue<DocumentOrderedScoreIterator>(iterators.length,
                new ScoreOrderComparator());
        weightLookup = new TObjectDoubleHashMap();
        for (int i = 0; i < iterators.length; i++) {
            DocumentOrderedScoreIterator dosi = iterators[i];
            weightLookup.put(dosi, weights[i]);
            if (dosi.getClass().isAssignableFrom(TopDocsScoringIterator.class)) {
                topdocs.add(dosi);
            } else {
                normal.add(dosi);
            }
        }
        cacheScores();
        for (int i = 0; i < iterators.length; i++) {
            DocumentOrderedScoreIterator dosi = iterators[i];
            potential += weights[i] * dosi.maximumScore();
            minimum += weights[i] * dosi.minimumScore();
            // don't normalize
        }
        selectNextDocument(0);
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
        double remainingPotential = 0.0;
        boolean haveOpenIterators = false;
        for (DocumentOrderedScoreIterator dosi : iterators) {
            if (!dosi.isDone()) {
                haveOpenIterators = true;
                remainingPotential += dosi.maximumScore();
            }
        }

        if (!haveOpenIterators) {
            return true;
        }
        if (scored >= requested && remainingPotential < max_score) {
            return true;
        }
        return false;
    }

    /**
     * This will be set after selection. If requested > scored, it's simply the
     * next document across all iterators. Otherwise it had to make the cut.
     * @return
     */
    @Override
    public int currentCandidate() {
        return currentDocument;
    }

    /**
     * True if the currentDocument equals the parameter. False otherwise.
     * @param document
     * @return
     */
    @Override
    public boolean hasMatch(int document) {
        return (currentDocument == document);
    }

    /**
     *  We proactively score in order to prune, so
     * we only use this opportunity to update the bookkeeping
     */
    public double score() {
        return score(documentToScore, lengthOfDocumentToScore);
    }

    public double score(int document, int length) {
        if (document == currentDocument) {
            if (currentScore > max_score) {
                max_score = currentScore;
            }
            scored = Math.min(scored + 1, requested);
            return currentScore;
        } else {
            // We didn't match, and we think the document being asked for is
            // garbage.
            return minimum;
        }
    }

    /**
     * Uses context-free scoring (i.e. the parameters are explicitly passed in)
     * @return
     */
    private double fullyScore(int document, int length) {
        double total = 0;

        for (int i = 0; i < iterators.length; i++) {
            total += weights[i] * iterators[i].score(document, length);
        }
        return total / weightSum;
    }

    private double partialScore(Collection<DocumentOrderedScoreIterator> scorers, int document,
            int length) {
        double total = 0;
        for (DocumentOrderedScoreIterator dosi : scorers) {
            double weight = weightLookup.get(dosi);
            total += (weight * dosi.score(document, length));
        }
        return total;
    }

    /**
     * Moves past the parameter supplied. If scored > requested,
     * also tries to find the next viable document via max_score pruning,
     * otherwise behavior is unchanged.
     * @param document
     * @throws IOException
     */
    public void movePast(int document) throws IOException {
        selectNextDocument(document + 1);
    }

    public void moveTo(int document) throws IOException {
        selectNextDocument(document);
    }

    public boolean skipToDocument(int document) throws IOException {
        selectNextDocument(document);
        return (hasMatch(document));
    }

    /**
     * Resets all iterators, including the topdocs iterators
     * @throws IOException
     */
    public void reset() throws IOException {
        for (DocumentOrderedScoreIterator dosi : iterators) {
            dosi.reset();
        }
    }

    /**
     * We pre-evaluate the the topdocs lists by scoring them against all iterators
     * that have topdocs, giving us a partial evaluation, and therefore a better
     * max_score bound - we also move the scored count up in order to activate
     * max_score sooner.
     */
    protected void cacheScores() throws IOException {
        // Iterate over the lists, scoring as we go - they are doc-ordered after all
        PriorityQueue<TopDocsReader.Iterator> toScore = new PriorityQueue<TopDocsReader.Iterator>();
        HashMap<TopDocsReader.Iterator, TopDocsScoringIterator> lookup =
                new HashMap<TopDocsReader.Iterator, TopDocsScoringIterator>();
        TIntDoubleHashMap scores = new TIntDoubleHashMap();
        for (DocumentOrderedScoreIterator dosi : topdocs) {
            TopDocsScoringIterator tdsi = (TopDocsScoringIterator) dosi;
            TopDocsReader.Iterator it = tdsi.getTopDocs();
            toScore.add(it);
            lookup.put(it, tdsi);
        }

        double score;
        boolean fullMatch;
        // Now we can simply iterate until done
        while (!toScore.peek().isDone()) {
            fullMatch = true;
            TopDocsReader.Iterator it = toScore.poll();
            TopDocument td = it.getCurrentTopDoc();
            // Score it using this iterator
            score = weightLookup.get(it)
                    * lookup.get(it).getScoringFunction().score(td.count, td.length);
            scores.put(td.document, score);
            lookup.get(it).lowerMaximumScore(score);
            it.movePast(td.document);
            // Now score w/ the others
            for (TopDocsReader.Iterator tdrit : toScore) {
                if (tdrit.hasMatch(td.document)) {
                    TopDocument other = tdrit.getCurrentTopDoc();
                    score = weightLookup.get(tdrit)
                            * lookup.get(tdrit).getScoringFunction().score(other.count, other.length);
                    scores.adjustValue(other.document, score);
                    lookup.get(tdrit).lowerMaximumScore(score);
                    tdrit.movePast(other.document); // scored - move on
                } else {
                    // background
                    scores.adjustValue(td.document, weightLookup.get(tdrit)
                            * lookup.get(tdrit).getScoringFunction().score(0, td.length));
                    fullMatch = false;
                }
            }
            toScore.add(it); // put it back in, do it again
            topdocsCandidates.add(td.document);

            if (fullMatch) { // we can cache this score
                Placeholder p = new Placeholder();
                p.document = td.document;
                p.score = scores.get(td.document);
                cache.add(p);
            }
        }

        // now create all the placeholders
        double[] values = scores.getValues();
        Arrays.sort(values);
        if (values.length < requested) {
            max_score = values[0]; // first is lowest
        } else {
            max_score = values[values.length - requested];
        }
        scored += scores.size();
    }

    /**
     * Method used to move all children iterators forward to the next viable
     * candidate. When we are below threshold, we move forward to the next candidate
     * without prejudice, and score it proactively. Otherwise, we move forward through
     * candidates until we find one that can be fully scored. The hope is that we can omit
     * a large number of unnecessary documents during this pruning period.
     *
     * @param startingPoint
     * @throws IOException
     */
    private void selectNextDocument(int startingPoint) throws IOException {
        if (isDone()) return;
        boolean outcome;
        int idx = startingPoint;
        do {
            outcome = foundNextDocument(idx);
            idx = currentDocument + 1; // in case we need it for the next round
        } while (outcome == false && !isDone());
    }

    /**
     * One iteration of trying to score the "next" document. Returns true if
     * the document was fully scored successfully, otherwise false.
     * @param startingPoint
     * @return
     * @throws IOException
     */
    private boolean foundNextDocument(int startingPoint) throws IOException {
        // move all the iterators forward
        currentDocument = Integer.MAX_VALUE;
        for (DocumentOrderedScoreIterator dosi : iterators) {
            dosi.moveTo(startingPoint);
            currentDocument = Math.min(currentDocument, dosi.currentCandidate());
        }

        // Move the lengths iterator and pull out the length
        docLengths.skipTo(currentDocument);
        int length = docLengths.getDocumentData().textLength;

        // First check the cache
        if (cache.peek().document == currentDocument) {
            // Success - can score fast, and it's a topdocs candidate
            Placeholder p = cache.poll();
            currentScore = p.score + partialScore(normal, currentDocument, length);
            currentScore /= weightSum;
            // done
            return true;
        } else if (topdocsCandidates.contains(currentDocument)
                || scored < requested) {
            // either we have to do it, or we don't have enough evidence yet
            currentScore = fullyScore(currentDocument, length);
            return true;
        } else { // MAX_SCORE PRUNING
            scoreQueue.clear();
            scoreQueue.addAll(normal);
            scoreQueue.addAll(topdocs);
            double runningScore = potential;
            double weight;
            while (!scoreQueue.isEmpty() && runningScore > max_score) {
                DocumentOrderedScoreIterator top = scoreQueue.poll();
                weight = weightLookup.get(top);
                // We have to remove the default score in there, effectively lowering
                // the potential for this document
                runningScore += (weight * (top.score(currentDocument, length) - top.maximumScore()));
            }

            if (!scoreQueue.isEmpty()) {
                return false;
            } else {
                currentScore = runningScore / weightSum;
                return true;
            }
        }
    }
}
