/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.scoring;

import gnu.trove.TObjectIntHashMap;
import gnu.trove.TObjectIntProcedure;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.galagosearch.core.index.PositionIndexReader;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.index.StructuredIndexPartReader;
import org.galagosearch.core.parse.CorpusReader;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.parse.TagTokenizer;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.util.TextPartAssigner;
import org.galagosearch.tupleflow.Parameters;

/**
 *  * Implements the query expansion of BM25 as described in
 * "Technique for Efficient Query Expansion" by Billerbeck and Zobel.
 * The weighting model itself is called "Term Selection Value".
 *
 *
 * @author irmarc
 */
public class TermSelectionValueModel implements ExpansionModel {
    // This version of the gram contains all contextual information
    // for the modified scoring algorithm.

    public static class Gram implements Comparable<Gram> {

        public String term;
        public double score;
        public int R;
        public int rt;

        public Gram(String t) {
            term = t;
            score = 0.0;
        }

        public int compareTo(Gram that) {
            return (this.score > that.score ? -1 : (this.score < that.score ? 1 : 0));
        }
    }
    Parameters parameters;
    CorpusReader cReader = null;
    TagTokenizer tokenizer = null;
    StructuredIndexPartReader reader = null;
    long N = 0;
    int fbDocs;

    public TermSelectionValueModel(Parameters parameters) {
        this.parameters = parameters;
    }

    public void initialize() throws IOException {
        this.N = parameters.get("documentCount", 0L);
        this.fbDocs = (int) parameters.get("fbDocs", 10L);

        if (cReader == null) {
            // Let's make a corpus reader
          String corpusLocation = parameters.get("corpus", null);
          if (corpusLocation == null) { // keep trying
            corpusLocation = parameters.get("index") + File.separator + "corpus";
          }
          cReader = new CorpusReader(corpusLocation);
        }

        if (tokenizer == null) {
            tokenizer = new TagTokenizer();
        }

        // Finally, we need an iterator from the index for the doc. frequencies
        // For now we only take PositionIndexReader.Iterators, meaning we need
        // a dummy text node
        Node dummy = TextPartAssigner.assignPart(new Node(), parameters);
        String indexPart = parameters.get("index") + File.separator + "parts"
                + File.separator + dummy.getParameters().get("part");
        reader = StructuredIndex.openIndexPart(indexPart);
    }

    public void cleanup() throws IOException {
        cReader.close();
        reader.close();
        cReader = null;
        reader = null;
    }

    public ArrayList<Gram> generateGrams(List<ScoredDocument> initialResults) throws IOException {
        // Count the dfs of the terms relative to the fb docs
        TObjectIntHashMap counts = countRFDF(initialResults);

        // now get collection-wide dfs, and calculate the TSVs.
        ArrayList<Gram> scored = scoreGrams(counts);
        Collections.sort(scored);
        return scored;
    }

    public Node generateExpansionQuery(List<ScoredDocument> initialResults, int fbTerms,
            Set<String> exclusionTerms) throws IOException {
        List<Gram> scored = generateGrams(initialResults);

        ArrayList<Node> children = new ArrayList<Node>();
        Parameters expParams = new Parameters();
        int expanded = 0;
        for (int i = 0; i < scored.size() && expanded < fbTerms; i++) {
            Gram g = scored.get(i);
            if (exclusionTerms.contains(g.term)) {
                continue;
            }
            Node inner = TextPartAssigner.assignPart(new Node("text", g.term), parameters);
            ArrayList<Node> innerChild = new ArrayList<Node>();
            innerChild.add(inner);
            Parameters weightParameters = new Parameters();
            weightParameters.set("default", "bm25rf");
            weightParameters.set("rt", Integer.toString(g.rt));
            weightParameters.set("R", Integer.toString(g.R));
            children.add(new Node("feature", weightParameters, innerChild, 0));
            expanded++;
        }
        Node newRoot = new Node("combine", expParams, children, 0);
        return newRoot;
    }

    protected TObjectIntHashMap countRFDF(List<ScoredDocument> results) throws IOException {
        TObjectIntHashMap counts = new TObjectIntHashMap();
        Document doc;
        HashSet<String> seen = new HashSet<String>();
        for (ScoredDocument sd : results) {
            doc = cReader.getDocument(sd.documentName);
            tokenizer.tokenize(doc);
            seen.clear();
            for (String term : doc.terms) {
                seen.add(term);
            }
            for (String term : seen) {
                counts.adjustOrPutValue(term, 1, 1);
            }
        }
        return counts;
    }

    protected ArrayList<Gram> scoreGrams(TObjectIntHashMap counts) {
        ArrayList<Gram> grams = new ArrayList<Gram>();
        TObjectIntProcedure gramGenerator = new GramGenerator(grams, reader);
        counts.forEachEntry(gramGenerator);
        return grams;
    }

    public class GramGenerator implements TObjectIntProcedure {

        ArrayList<Gram> grams;
        int R;
        PositionIndexReader reader;

        public GramGenerator(ArrayList<Gram> g, StructuredIndexPartReader reader) {
            this.grams = g;
            this.R = fbDocs;
            this.reader = (PositionIndexReader) reader;
        }

        // Variable naming is consistent w/ the formula for TSV in the paper.
        public boolean execute(Object a, int rt) {
            Gram g = new Gram((String) a);
            try {
                // Get df
                int ft = reader.documentCount(g.term);
                double partone = java.lang.Math.pow(((ft + 0.0) / N), rt);
                double parttwo = org.galagosearch.core.util.Math.binomialCoeff(R, rt);
                g.score = partone * parttwo;
                g.rt = rt;
                g.R = R;
                grams.add(g);
                return true;
            } catch (IOException ioe) {
                throw new RuntimeException("Unable to retrieval doc count for term: " + g.term, ioe);
            }
        }
    }
}
