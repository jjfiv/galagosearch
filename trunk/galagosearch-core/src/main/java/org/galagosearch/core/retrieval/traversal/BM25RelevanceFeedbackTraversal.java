package org.galagosearch.core.retrieval.traversal;

import gnu.trove.TObjectIntHashMap;
import gnu.trove.TObjectIntProcedure;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.galagosearch.core.index.PositionIndexReader;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.index.StructuredIndexPartReader;
import org.galagosearch.core.parse.CorpusReader;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.parse.TagTokenizer;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.core.util.TextPartAssigner;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 * Implements the query expansion of BM25 as described in
 * "Technique for Efficient Query Expansion" by Billerbeck and Zobel
 *
 * We run the query as a combine on the way back up, and add in the
 * expansion terms. This is similar to the RelevanceModelTraversal.
 *
 * Little weird here - we transform an operator over a subtree into
 * low-level feature operators that act on count iterators.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"retrievalGroup", "index", "documentCount"})
public class BM25RelevanceFeedbackTraversal implements Traversal {

    Retrieval retrieval;
    Parameters queryParameters;
    Parameters availableParts;
    TagTokenizer tokenizer;
    long N;

    public BM25RelevanceFeedbackTraversal(Parameters parameters, Retrieval retrieval) throws IOException {
        this.retrieval = retrieval;
        this.queryParameters = parameters;
        this.availableParts = retrieval.getAvailiableParts(parameters.get("retrievalGroup"));
        this.N = parameters.get("documentCount", 0L);
        tokenizer = new TagTokenizer();
    }

    public Node afterNode(Node original) throws Exception {
        if (original.getOperator().equals("bm25rf") == false) {
            return original;
        }

        // Kick off the inner query
        Parameters parameters = original.getParameters();
        int fbDocs = (int) parameters.get("fbDocs", 10);
        Node combineNode = new Node("combine", original.getInternalNodes());
        ArrayList<ScoredDocument> initialResults = new ArrayList<ScoredDocument>();

        // Only get as many as we need
        Parameters localParameters = queryParameters.clone();
        localParameters.set("requested", Integer.toString(fbDocs));

        retrieval.runAsynchronousQuery(combineNode, localParameters, initialResults);

        // while that's running, extract the feedback parameters
        int fbTerms = (int) parameters.get("fbTerms", 10);

        // Let's also make a corpus reader
        String corpusLocation = queryParameters.get("corpus", null);
        if (corpusLocation == null) { // keep trying
            corpusLocation = queryParameters.get("index") + File.separator + "corpus";
        }
        CorpusReader cReader = new CorpusReader(corpusLocation);

        // Finally, we need an iterator from the index for the doc. frequencies
        // For now we only take PositionIndexReader.Iterators, meaning we need
        // a dummy text node
        Node dummy = TextPartAssigner.assignPart(new Node(), availableParts);
        String indexPart = queryParameters.get("index") + File.separator + "parts"
                + File.separator + dummy.getParameters().get("part");
        StructuredIndexPartReader reader = StructuredIndex.openIndexPart(indexPart);
       
        // Now we wait
        retrieval.waitForAsynchronousQuery();

        // Count the dfs of the terms relative to the fb docs
        TObjectIntHashMap counts = countRFDF(initialResults, cReader);
        cReader.close();

        // now get collection-wide dfs, and calculate the TSVs.
        ArrayList<Gram> scored = scoreGrams(counts, reader, fbDocs);
        Collections.sort(scored);
        Node newRoot = null;

        // Don't need this anymore
        reader.close();
        reader = null;

        // Time to construct the modified query - start with the expansion since we always have it
        // make sure we filter stopwords
        HashSet<String> stopwords = Utility.readStreamToStringSet(getClass().getResourceAsStream("/stopwords/inquery"));
        Set<String> queryTerms = StructuredQuery.findQueryTerms(combineNode, "extents");
        stopwords.addAll(queryTerms);
        Parameters expParams = new Parameters();
        ArrayList<Node> initialChildren = original.getInternalNodes();
        int expanded = 0;
        for (int i = 0; i < scored.size() && expanded < fbTerms; i++) {
            Gram g = scored.get(i);
            if (stopwords.contains(g.term)) {
                continue;
            }
            Node inner = TextPartAssigner.assignPart(new Node("text", g.term), availableParts);
            ArrayList<Node> innerChild = new ArrayList<Node>();
            innerChild.add(inner);
            Parameters weightParameters = new Parameters();
            weightParameters.set("default", "bm25rf");
            weightParameters.set("rt", Integer.toString(g.rt));
            weightParameters.set("R", Integer.toString(g.R));
            initialChildren.add(new Node("feature", weightParameters, innerChild, 0));
            expanded++;
        }
        newRoot = new Node("combine", expParams, initialChildren, 0);
        return newRoot;
    }

    public void beforeNode(Node object) throws Exception {
        // do nothing
    }

    protected TObjectIntHashMap countRFDF(ArrayList<ScoredDocument> results, CorpusReader reader) throws IOException {
        TObjectIntHashMap counts = new TObjectIntHashMap();
        Document doc;
        HashSet<String> seen = new HashSet<String>();
        for (ScoredDocument sd : results) {
            doc = reader.getDocument(sd.documentName);
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

    protected ArrayList<Gram> scoreGrams(TObjectIntHashMap counts, StructuredIndexPartReader reader, int fbDocs) {
        ArrayList<Gram> grams = new ArrayList<Gram>();
        TObjectIntProcedure gramGenerator = new GramGenerator(grams, reader, fbDocs);
        counts.forEachEntry(gramGenerator);
        return grams;
    }

    public class GramGenerator implements TObjectIntProcedure {

        ArrayList<Gram> grams;
        int R;
        PositionIndexReader reader ;

        public GramGenerator(ArrayList<Gram> g, StructuredIndexPartReader reader, int fbDocs) {
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
            double partone = java.lang.Math.pow(((ft+0.0)/N), rt);
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
}
