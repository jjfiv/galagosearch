/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.parse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;
import org.galagosearch.core.index.DocumentLengthsReader;
import org.galagosearch.core.index.PositionIndexReader;
import org.galagosearch.core.retrieval.structured.ExtentIndexIterator;
import org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.core.types.TopDocsEntry;
import org.galagosearch.tupleflow.FileSource;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;

/**
 *
 * @author marc
 */
@InputClass(className="org.galagosearch.core.types.KeyValuePair", order={"+key"})
@OutputClass(className="org.galagosearch.core.types.TopDocsEntry", order={"+word", "+document"})
public class TopDocsScanner extends StandardStep<KeyValuePair, TopDocsEntry> {

    public static class NWPComparator implements Comparator<TopDocsEntry> {
        public int compare(TopDocsEntry a, TopDocsEntry b) {
            int result = (a.probability > b.probability) ? 1 :
                ((a.probability < b.probability) ? -1 : 0);
            if (result != 0) return result;
            return (a.document - b.document);
        }
    }

    protected int size;
    protected long minlength;
    protected long count;
    PriorityQueue<TopDocsEntry> topdocs;
    PositionIndexReader partReader;
    NumberedDocumentDataIterator docLengths;
    DocumentLengthsReader docReader;
    ExtentIndexIterator extentIterator;
    TopDocsEntry tde;

    public TopDocsScanner(TupleFlowParameters parameters) throws IOException {
        size = (int) parameters.getXML().get("size", Integer.MAX_VALUE);
        minlength = parameters.getXML().get("minlength", Long.MAX_VALUE);
        topdocs = new PriorityQueue<TopDocsEntry>(size, new NWPComparator());
        String indexLocation = parameters.getXML().get("directory");
        docReader = new DocumentLengthsReader(indexLocation +
                File.separator + "documentLengths");
        docLengths = docReader.getIterator();
        partReader = new PositionIndexReader(indexLocation + File.separator +
                parameters.getXML().get("part"));
    }

    @Override
    public void process(KeyValuePair object) throws IOException {
        // Get out posting list
        count = 0;
        topdocs.clear();
        extentIterator = partReader.getTermCounts(Utility.toString(object.key));
        if (extentIterator instanceof PositionIndexReader.Iterator) {
            count = ((PositionIndexReader.Iterator) extentIterator).totalDocuments();
            if (count < minlength) return; //skip this
        }

        // And iterate
        while (!extentIterator.isDone()) {
            count++;
            docLengths.skipTo(extentIterator.currentCandidate());
            int length = docLengths.getDocumentData().textLength;
            double probability = (0.0+extentIterator.count())
                    / (0.0+length);
            tde = new TopDocsEntry();
            tde.document = extentIterator.currentCandidate();
            tde.count = extentIterator.count();
            tde.doclength = length;
            tde.probability = probability;
            topdocs.add(tde);

            // Keep it trimmed
            if (topdocs.size() > size) {
                topdocs.poll();
            }
            extentIterator.nextEntry();
        }

        // skip if it's too small
        if (count < minlength) {
            topdocs.clear();
            return;
        }

        // Now emit based on our top docs (have to reverse first)
        TopDocsEntry[] resort = new TopDocsEntry[size];

        for (int i = size-1; i > -1; i--) {
            resort[i] = topdocs.poll();
        }

        for (int i = 0; i < resort.length; i++) {
            TopDocsEntry entry = resort[i];
            entry.word = object.key;
            processor.process(entry);
        }
    }

    public void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        FileSource.verify(parameters, handler);
        if (!parameters.getXML().containsKey("size")) handler.addError("Need size.");
        if (!parameters.getXML().containsKey("minlength")) handler.addError("Need minlength");
        if (!parameters.getXML().containsKey("part")) handler.addError("Need index part");
    }

    public void close() throws IOException {
        docReader.close();
        partReader.close();
        processor.close();
    }
}
