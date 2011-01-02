package org.galagosearch.core.scoring;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.core.retrieval.query.Node;

/**
 *
 * Generic interface for an expander
 *
 * @author irmarc
 */
public interface ExpansionModel {

    public void initialize() throws Exception;
    public void cleanup() throws Exception;

    public List<? extends Object> generateGrams(List<ScoredDocument> initialResults)
            throws IOException;

    public Node generateExpansionQuery(List<ScoredDocument> initialResults, int fbTerms, 
            Set<String> exclusionTerms) throws IOException;
}
