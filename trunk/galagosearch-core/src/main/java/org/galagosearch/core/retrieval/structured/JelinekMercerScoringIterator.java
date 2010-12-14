/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.core.scoring.JelinekMercerScorer;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author marc
 */
@RequiredStatistics(statistics = {"collectionLength"})
public class JelinekMercerScoringIterator extends ScoringFunctionIterator {
    public JelinekMercerScoringIterator(Parameters p, DocumentOrderedCountIterator it)
            throws IOException {
        super(it, new JelinekMercerScorer(p, it));
    }

}
