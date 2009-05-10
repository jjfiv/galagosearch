// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.galagosearch.core.index.VocabularyReader;
import org.galagosearch.core.index.VocabularyReader.TermSlot;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.tupleflow.ExNihiloSource;
import org.galagosearch.tupleflow.FileSource;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verified;
import org.galagosearch.core.types.DocumentSplit;

/**
 * From a set of inputs, splits the input into many DocumentSplit records.
 * This will usually be in a stage by itself at the beginning of a Galago pipeline.
 * This is somewhat similar to FileSource, except that it can autodetect file formats.
 * This splitter can detect ARC, TREC, TRECWEB and corpus files.
 * 
 * @author trevor
 */

@Verified
@OutputClass(className = "org.galagosearch.core.types.DocumentSplit")
public class DocumentSource implements ExNihiloSource<DocumentSplit> {
    public Processor processor;
    TupleFlowParameters parameters;
    
    public DocumentSource(TupleFlowParameters parameters) {
        this.parameters = parameters;
    }

    private String getExtension(String fileName) {
        String[] fields = fileName.split("\\.");
        
        // A filename needs to have a period to have an extension.
        if (fields.length <= 1) {
            return "";
        }
        
        // If the last chunk of the filename is gz, we'll ignore it.
        // The second-to-last bit is the type extension (but only if
        // there are at least three parts to the name).
        if (fields[fields.length-1].equals("gz")) {
            if (fields.length > 2) {
                return fields[fields.length-2];
            } else {
                return "";
            }
        }
        
        // No 'gz' extension, so just return the last part.
        return fields[fields.length-1];
    }

    private void processCorpusFile(String fileName, String fileType) throws IOException {
        // If this is a big file, we'll split it into roughly 100MB pieces.
        long fileLength = new File(fileName).length();
        long chunkSize = 100 * 1024 * 1024;
        
        IndexReader reader = new IndexReader(fileName);
        VocabularyReader vocabulary = reader.getVocabulary();
        List<TermSlot> slots = vocabulary.getSlots();
        int pieces = Math.max(2, (int) (fileLength / chunkSize));
        ArrayList<byte[]> keys = new ArrayList<byte[]>();

        for (int i = 1; i < pieces; ++i) {
            float fraction = (float) i / pieces;
            int slot = (int) (fraction * slots.size());
            keys.add(slots.get(slot).termData);
        }

        for (int i = 0; i < pieces; ++i) {
            byte[] firstKey = new byte[0];
            byte[] lastKey = new byte[0];

            if (i > 0) {
                firstKey = keys.get(i - 1);
            }
            if (i < pieces - 1) {
                lastKey = keys.get(i);
            }
            DocumentSplit split = new DocumentSplit(fileName, fileType, false, firstKey, lastKey);
            processor.process(split);
        }
    }
    
    private void processFile(String fileName) throws IOException {
        // First, try to detect what kind of file this is:
        boolean isCompressed = fileName.endsWith(".gz");
        String fileType = null;
        
        // We'll try to detect by extension first, so we don't have to open the file
        String extension = getExtension(fileName);
        if (extension.equals("corpus") ||
            extension.equals("trecweb") ||
            extension.equals("trectext") ||
            extension.equals("arc") ||
            extension.equals("txt") ||
            extension.equals("html") ||
            extension.equals("xml")) {
            fileType = extension;
        } else {
            // Oh well, we need to autodetect the file type.
            if (IndexReader.isIndexFile(fileName)) {
                fileType = "corpus";
            } else {
                // Eventually it'd be nice to do more format detection here.
                System.err.println("Skipping: " + fileName);
                return;
            }
        }
        
        if (fileType.equals("corpus")) {
            processCorpusFile(fileName, fileType);            
        } else {
            processSplit(fileName, fileType, isCompressed);
        }
    }
    
    private void processDirectory(File root) throws IOException {
        for (File file : root.listFiles()) {
            if (file.isHidden()) {
                continue;
            }
            if (file.isDirectory()) {
                processDirectory(file);
            } else {
                processFile(file.getAbsolutePath());
            }
        }
    }
    
    public void run() throws IOException {
        if (parameters.getXML().containsKey("directory")) {
            List<Value> directories = parameters.getXML().list("directory");

            for (Value directory : directories) {
                File directoryFile = new File(directory.toString());
                processDirectory(directoryFile);
            }
        } else if (parameters.getXML().containsKey("filename")) {
            List<Value> files = parameters.getXML().list("filename");

            for (Value file : files) {
                processFile(file.toString());
            }
        }

        processor.close();
    }

    public void setProcessor(Step processor) throws IncompatibleProcessorException {
        Linkage.link(this, processor);
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        FileSource.verify(parameters, handler);
    }

    private void processSplit(String fileName, String fileType, boolean isCompressed) throws IOException {
        DocumentSplit split = new DocumentSplit(fileName, fileType, isCompressed, new byte[0], new byte[0]);
        processor.process(split);
    }
}
