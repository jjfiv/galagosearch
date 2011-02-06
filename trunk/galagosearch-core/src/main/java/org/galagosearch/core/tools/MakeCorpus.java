package org.galagosearch.core.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.galagosearch.core.parse.DocumentSource;
import org.galagosearch.core.parse.UniversalParser;
import org.galagosearch.core.index.parallel.ParallelIndexKeyWriter;
import org.galagosearch.core.index.parallel.ParallelIndexValueWriter;
import org.galagosearch.core.index.corpus.CorpusReader;
import org.galagosearch.core.index.corpus.CorpusWriter;
import org.galagosearch.core.index.corpus.DocumentToKeyValuePair;
import org.galagosearch.core.index.corpus.DocumentIndexWriter;
import org.galagosearch.core.index.corpus.KeyValuePairToDocument;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ConnectionAssignmentType;
import org.galagosearch.tupleflow.execution.ConnectionPointType;
import org.galagosearch.tupleflow.execution.InputStep;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.execution.OutputStep;
import org.galagosearch.tupleflow.execution.Stage;
import org.galagosearch.tupleflow.execution.StageConnectionPoint;
import org.galagosearch.tupleflow.execution.Step;

/*
 * @author sjh
 * 
 * new corpus structure;
 *  - contained within some folder.
 *  - corpus data is stored in some set of files (.cds)
 *  - index file stores: document-name --> (file, offset)
 * 
 */
public class MakeCorpus {

    String indexunit;
    Parameters corpusParameters;

    public Stage getSplitStage(ArrayList<String> inputs) throws IOException {
        Stage stage = new Stage("inputSplit");
        stage.add(new StageConnectionPoint(ConnectionPointType.Output, "splits",
                new DocumentSplit.FileIdOrder()));

        Parameters p = new Parameters();
        for (String input : inputs) {
            File inputFile = new File(input);

            if (inputFile.isFile()) {
                p.add("filename", inputFile.getAbsolutePath());
            } else if (inputFile.isDirectory()) {
                p.add("directory", inputFile.getAbsolutePath());
            } else {
                throw new IOException("Couldn't find file/directory: " + input);
            }
        }

        stage.add(new Step(DocumentSource.class, p));
        stage.add(Utility.getSorter(new DocumentSplit.FileIdOrder()));
        stage.add(new OutputStep("splits"));

        return stage;
    }

    public Stage getParseWriteDocumentsStage() {
        Stage stage = new Stage("parserWriter");

        stage.add(new StageConnectionPoint(
                ConnectionPointType.Input,
                "splits", new DocumentSplit.FileIdOrder()));

        stage.add(new StageConnectionPoint(
                ConnectionPointType.Output,
                "indexData", new KeyValuePair.KeyOrder()));

        stage.add(new InputStep("splits"));

        Parameters p = new Parameters();
        p.add("indexunit", this.indexunit);
        stage.add(new Step(UniversalParser.class, p));

        stage.add( new Step( CorpusWriter.class, corpusParameters.clone() ));
        stage.add(Utility.getSorter(new KeyValuePair.KeyOrder()));
        stage.add(new OutputStep("indexData"));

        return stage;
    }

    public Stage getIndexWriterStage() {
        Stage stage = new Stage("indexWriter");

        stage.add(new StageConnectionPoint(ConnectionPointType.Input,
                "indexData", new KeyValuePair.KeyOrder()));

        stage.add(new InputStep("indexData"));
        stage.add(new Step(ParallelIndexKeyWriter.class, corpusParameters.clone()));

        return stage;
    }

    public static Job getCorpusFileJob(String outputCorpus, ArrayList<String> inputs) throws IOException {
        Job job = new Job();

        Stage stage = new Stage("split");
        stage.add(new StageConnectionPoint(ConnectionPointType.Output, "docs",
                new KeyValuePair.KeyOrder()));
        Parameters p = new Parameters();
        for (String input : inputs) {
            File inputFile = new File(input);

            if (inputFile.isFile()) {
                p.add("filename", input);
            } else if (inputFile.isDirectory()) {
                p.add("directory", input);
            } else {
                throw new IOException("Couldn't find file/directory: " + input);
            }
        }

        stage.add(new Step(DocumentSource.class, p));
        p = new Parameters();
        p.add("identifier", "stripped");
        stage.add(new Step(UniversalParser.class, p));
        stage.add(new Step(DocumentToKeyValuePair.class));
        stage.add(Utility.getSorter(new KeyValuePair.KeyOrder()));
        stage.add(new OutputStep("docs"));
        job.add(stage);

        stage = new Stage("docwrite");
        stage.add(new StageConnectionPoint(ConnectionPointType.Input, "docs",
                new KeyValuePair.KeyOrder()));
        stage.add(new InputStep("docs"));
        stage.add(new Step(KeyValuePairToDocument.class));
        p = new Parameters();
        p.add("filename", outputCorpus);
        stage.add(new Step(DocumentIndexWriter.class, p));

        job.add(stage);
        job.connect("split", "docwrite", ConnectionAssignmentType.Combined);
        return job;
    }

    public Job getMakeCorpusJob(Parameters p) throws IOException {

        ArrayList<String> inputPaths = new ArrayList();
        List<Value> vs = p.list("inputPaths");
        for (Value v : vs) {
            inputPaths.add(v.toString());
        }

        File corpus = new File(p.get("corpusPath"));


        // check if we're creating a single file corpus
        if (p.get("corpusFormat", "folder").equals("file")) {
            return getCorpusFileJob(corpus.getAbsolutePath(), inputPaths);
        }


        // otherwise we're creating a folder corpus
        if (corpus.isFile()) {
            corpus.delete();
        } else if (corpus.isDirectory()) {
            Utility.deleteDirectory(corpus);
        }

        this.indexunit = p.get("indexunit", "");
        this.corpusParameters = new Parameters();
        this.corpusParameters.add("compressed", p.get("compressed", "true"));
        this.corpusParameters.add("filename", corpus.getAbsolutePath());
        this.corpusParameters.add("readerClass", CorpusReader.class.getName());
        this.corpusParameters.add("writerClass", CorpusWriter.class.getName());

        Job job = new Job();

        job.add(getSplitStage(inputPaths));
        job.add(getParseWriteDocumentsStage());
        job.add(getIndexWriterStage());

        job.connect("inputSplit", "parserWriter", ConnectionAssignmentType.Each);
        job.connect("parserWriter", "indexWriter", ConnectionAssignmentType.Combined);

        return job;
    }
}
