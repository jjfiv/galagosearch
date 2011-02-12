// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.corpus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.zip.GZIPOutputStream;
import org.galagosearch.core.index.GenericElement;
import org.galagosearch.core.parse.Document;

import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Source;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * Writes documents to a file
 *  - new output file is created in the folder specified by "filename"
 *  - document.identifier -> output-file, byte-offset is passed on
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.parse.Document")
@OutputClass(className = "org.galagosearch.core.types.KeyValuePair")
public class CorpusWriter implements Processor<Document>, Source<KeyValuePair> {

    boolean compressed;
    SplitIndexValueWriter writer;

    public CorpusWriter(TupleFlowParameters parameters) throws IOException, IncompatibleProcessorException {
        compressed = parameters.getXML().get("compressed", true);

        // create a writer;
        parameters.getXML().add("readerClass", CorpusReader.class.getName());
        parameters.getXML().add("writerClass", CorpusWriter.class.getName());
        writer = new SplitIndexValueWriter( parameters );
        // note that the setProcessor function needs to be modified!
    }

    public void process(Document document) throws IOException {
        ByteArrayOutputStream array = new ByteArrayOutputStream();
        ObjectOutputStream output;
        if (compressed) {
            output = new ObjectOutputStream(new GZIPOutputStream(array));
        } else {
            output = new ObjectOutputStream(array);
        }

        output.writeObject(document);
        output.close();

        byte[] key = Utility.fromString(document.identifier);
        byte[] value = array.toByteArray();
        GenericElement elem = new GenericElement(key, value);
        writer.add(elem);
   }
    
    public void close() throws IOException{
        writer.close();
    }

    public void setProcessor(Step next) throws IncompatibleProcessorException {
        Linkage.link(writer, next);
    }
}
