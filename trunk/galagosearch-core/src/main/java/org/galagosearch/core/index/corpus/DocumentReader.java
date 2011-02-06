// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.corpus;

import org.galagosearch.core.index.corpus.DocumentIndexReader;
import org.galagosearch.core.index.corpus.CorpusReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.core.index.parallel.ParallelIndexReader;
import org.galagosearch.core.parse.Document;

/**
 * Interface that allows different corpus formats
 * See CorpusReader/DocumentIndexReader
 *
 * @author sjh
 */
public abstract class DocumentReader {

    public abstract void close() throws IOException;

    public abstract DocumentIterator getIterator() throws IOException;

    public abstract Document getDocument(String key) throws IOException;

    public abstract interface DocumentIterator {

        public void skipTo(byte[] key) throws IOException;

        public String getKey();

        public boolean isDone();

        public Document getDocument() throws IOException;

        public boolean nextDocument() throws IOException;
    }

    public static DocumentReader getInstance(String path) throws IOException {
        if (isCorpus(path)) {
            System.err.println("Folder Corpus");
            return new CorpusReader(path);
        } else if (IndexReader.isIndexFile(path)) {
            System.err.println("File Corpus");
            return new DocumentIndexReader(path);
        } else {
            throw new IOException("Path is not a known corpus format: " + path);
        }
    }

    /*
     * Checks if it is a corpus folder structure
     *  - file structure can be checked using isIndexFile(path)
     */
    public static boolean isCorpus(String fileName) throws IOException {
        return ParallelIndexReader.isParallelIndex(fileName);
    }
}
