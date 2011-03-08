/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class FrequentCountIterator extends DocumentOrderedCountIterator {
    
    int threshold;
    CountIterator i;

    public FrequentCountIterator(Parameters parameters, DocumentOrderedCountIterator iterator){
        i = iterator;
        threshold = (int) parameters.get("default", 0);
    }

    public int document() {
        return i.document();
    }

    public int count() {
        int c = i.count();
        if(c > threshold){
            return c;
        } else {
            return 0;
        }
    }

    public boolean isDone() {
        return i.isDone();
    }

    public void nextEntry() throws IOException {
        i.nextEntry();
    }

    public void reset() throws IOException {
        i.reset();
    }
}
