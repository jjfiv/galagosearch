// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.types.FileName;

/**
 *
 * @author trevor
 */
@OutputClass(className = "org.galagosearch.tupleflow.types.FileName", order = {"+filename"})
public class FileSource implements ExNihiloSource<FileName> {
    TupleFlowParameters parameters;
    public Processor<FileName> processor;

    /** Creates a new instance of FileSource */
    public FileSource(TupleFlowParameters parameters) {
        this.parameters = parameters;
    }

    private void processDirectory(File root) throws IOException {
        for (File file : root.listFiles()) {
            if (file.isHidden()) {
                continue;
            }
            if (file.isDirectory()) {
                processDirectory(file);
            } else {
                processor.process(new FileName(file.toString()));
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
                String filename = file.toString();
                processor.process(new FileName(filename));
            }
        }

        processor.close();
    }

    public void close() throws IOException {
        processor.close();
    }

    public void setProcessor(Step nextStage) throws IncompatibleProcessorException {
        Linkage.link(this, nextStage);
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        if (!(parameters.getXML().containsKey("directory") || parameters.getXML().containsKey("filename"))) {
            handler.addError("FileSource requires either at least one directory or filename parameter.");
            return;
        }

        if (parameters.getXML().containsKey("directory")) {
            List<Value> directories = parameters.getXML().list("directory");

            for (Value directory : directories) {
                File directoryFile = new File(directory.toString());

                if (directoryFile.exists() == false) {
                    handler.addError("Directory " + directoryFile.toString() + " doesn't exist.");
                } else if (directoryFile.isDirectory() == false) {
                    handler.addError(directoryFile.toString() + " exists, but it isn't a directory.");
                }
            }
        } else if (parameters.getXML().containsKey("filename")) {
            List<Value> files = parameters.getXML().list("filename");

            for (Value file : files) {
                File f = new File(file.toString());

                if (f.exists() == false) {
                    handler.addError("File " + file.toString() + " doesn't exist.");
                } else if (f.isFile() == false) {
                    handler.addError(file.toString() + " exists, but isn't a file.");
                }
            }
        }
    }
}
