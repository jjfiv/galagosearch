// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;

/**
 * Writes a list of document names to a binary file.
 * This class assumes that a document name is a string that contains at least
 * one hyphen ('-') followed entirely by numbers.  All TREC document names
 * follow this convention, e.g.:  WTX-B01-0001.
 *
 * @author Trevor Strohman
 */
@InputClass(className = "org.galagosearch.core.types.NumberedDocumentData")
public class DocumentNameWriter implements Processor<NumberedDocumentData> {
    String lastHeader = null;
    DataOutputStream output;
    int lastFooterWidth = 0;
    int lastDocument = -1;
    ArrayList<Integer> footers;
    Counter documentsWritten = null;

    public DocumentNameWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
        String filename = parameters.getXML().get("filename");
        footers = new ArrayList<Integer>();
        Utility.makeParentDirectories(filename);
        output = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
        documentsWritten = parameters.getCounter("Documents Written");
    }

    public void flush() throws IOException {
        if (footers.size() == 0) {
            return;
        }

        byte[] headerBytes = Utility.makeBytes(lastHeader);
        output.writeInt(headerBytes.length);
        output.write(headerBytes);
        output.writeInt(lastFooterWidth);
        output.writeInt(footers.size());

        for (int footerValue : footers) {
            output.writeInt(footerValue);
        }
    }

    public void process(NumberedDocumentData numberedDocumentData) throws IOException {
        assert numberedDocumentData.number - 1 == lastDocument;
        lastDocument = numberedDocumentData.number;

        String documentName = numberedDocumentData.identifier;
        int lastDash = documentName.lastIndexOf("-");

        if (lastDash == -1) {
            putName(documentName, 0, 0);
        } else {
            String header = documentName.substring(0, lastDash);
            String footer = documentName.substring(lastDash + 1);

            try {
                int footerValue = Integer.parseInt(footer);
                putName(header, footerValue, footer.length());
            } catch (NumberFormatException e) {
                putName(documentName, 0, 0);
            }
        }

        if (documentsWritten != null) documentsWritten.increment();
    }

    public void putName(String header, int footer, int footerWidth) throws IOException {
        if (header.equals(lastHeader) && footerWidth == lastFooterWidth) {
            footers.add(footer);
        } else {
            flush();
            lastHeader = header;
            footers = new ArrayList<Integer>();
            footers.add(footer);
            lastFooterWidth = footerWidth;
        }
    }

    public void close() throws IOException {
        flush();
        output.close();
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        if (!parameters.getXML().containsKey("filename")) {
            handler.addError("DocumentNameWriter requires an 'filename' parameter.");
            return;
        }
    }
}
