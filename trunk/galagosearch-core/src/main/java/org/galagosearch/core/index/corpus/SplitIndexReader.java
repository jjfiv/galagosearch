/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.index.corpus;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.galagosearch.core.index.GenericIndexReader;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.core.index.VocabularyReader;
import org.galagosearch.tupleflow.BufferedFileDataStream;
import org.galagosearch.tupleflow.DataStream;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.StreamCreator;
import org.galagosearch.tupleflow.Utility;

/**
 * Split index reader
 *  - Index is a mapping from byte[] to byte[]
 *
 *  - allows values to be written out of order to a set of files
 *  - a unified ordered key structure should be kept in a folder
 *    with these value files, as created by SplitIndexKeyWriter
 *  - SplitIndexReader will read this data
 *
 *  This class if useful for writing a corpus structure
 *  - documents can be written to disk in any order
 *  - the key structure allows the documents to be found quickly
 *  - class is more efficient if the
 *    documents are inserted in sorted order
 *
 * @author sjh
 */
public class SplitIndexReader extends GenericIndexReader {
    public static final long VALUE_FILE_MAGIC_NUMBER = 0x2b3c4d5e6f7a8b9cL;

    RandomAccessFile[] dataFiles;
    IndexReader vocabIndex;
    String indexFolder;
    int hashMod;

    public class Iterator extends GenericIndexReader.Iterator {

        IndexReader.Iterator vocabIterator;
        boolean valueLoaded = false;
        int file;
        long valueOffset;
        long valueLength;

        public Iterator(IndexReader.Iterator vocabIterator) {
            this.vocabIterator = vocabIterator;
            assert (this.vocabIterator != null);
        }

        /**
         * Returns the data file containing the current value
         */
        public RandomAccessFile getInput() throws IOException {
            if (!valueLoaded) {
                loadValue();
            }
            return dataFiles[file];
        }

        /**
         * Returns the key associated with the current inverted list.
         */
        public byte[] getKey() {
            return vocabIterator.getKey();
        }

        /*
         * Skip iterator to the provided key
         */
        public void skipTo(byte[] key) throws IOException {
            vocabIterator.skipTo(key);
            valueLoaded = false;
        }

        /**
         * Advances to the next key in the index.
         */
        public boolean nextKey() throws IOException {
            valueLoaded = false;
            return vocabIterator.nextKey();
        }

        /**
         * Returns true if no more keys remain to be read.
         */
        public boolean isDone() {
            return vocabIterator.isDone();
        }

        /**
         * Returns the byte offset of the end of this block.
         */
        public long getFilePosition() throws IOException {
            if (!valueLoaded) {
                loadValue();
            }
            return valueOffset;
        }

        /**
         * Returns the length of the value, in bytes.
         */
        public long getValueLength() throws IOException {
            if (!valueLoaded) {
                loadValue();
            }

            return valueLength;
        }

        /**
         * Returns the value as a buffered stream.
         */
        public DataStream getValueStream() throws IOException {
            if (!valueLoaded) {
                loadValue();
            }

            return new BufferedFileDataStream(dataFiles[file], getValueStart(), getValueEnd());
        }

        /**
         * Returns the byte offset
         * of the beginning of the current inverted list,
         * relative to the start of the whole inverted file.
         */
        public long getValueStart() throws IOException {
            if (!valueLoaded) {
                loadValue();
            }
            return valueOffset;
        }

        /**
         * Returns the byte offset
         * of the end of the current inverted list,
         * relative to the start of the whole inverted file.
         */
        public long getValueEnd() throws IOException {
            if (!valueLoaded) {
                loadValue();
            }
            return valueOffset + valueLength;
        }


        //**********************//
        // local functions

        /**
         * Reads the header information for a data value
         *
         * @throws IOException
         */
        private void loadValue() throws IOException {
            valueLoaded = true;
            DataStream stream = vocabIterator.getValueStream();

            file = stream.readInt();
            valueOffset = stream.readLong();
            valueLength = stream.readLong();

            if (dataFiles[file] == null) {
                dataFiles[file] = StreamCreator.inputStream(indexFolder + File.separator + file);
            }
            // dataFiles[file].seek(valueOffset);
        }
    }

    /*
     * Constructors
     */
    public SplitIndexReader(String filename) throws IOException {
        File f = new File(filename);
        if (f.isDirectory()) {
            f = new File(filename + File.separator + "key.index");
        }
        vocabIndex = new IndexReader(f);

        indexFolder = f.getParent();
        //  (-1) for the key index
        hashMod = f.getParentFile().list().length - 1;

        dataFiles = new RandomAccessFile[hashMod];
    }

    /**
     * Returns a Parameters object that contains metadata about
     * the contents of the index.  This is the place to store important
     * data about the index contents, like what stemmer was used or the
     * total number of terms in the collection.
     */
    public Parameters getManifest() {
        return vocabIndex.getManifest();
    }

    /**
     * Returns the vocabulary structure for this IndexReader.  Note that the vocabulary
     * contains only the first key in each block.
     */
    public VocabularyReader getVocabulary() {
        return vocabIndex.getVocabulary();
    }

    /**
     * Returns an iterator pointing to the very first key in the index.
     * This is typically used for iterating through the entire index,
     * which might be useful for testing and debugging tools, but probably
     * not for traditional document retrieval.
     */
    public Iterator getIterator() throws IOException {
        return new Iterator(vocabIndex.getIterator());
    }

    /**
     * Returns an iterator pointing at a specific key.  Returns
     * null if the key is not found in the index.
     */
    public Iterator getIterator(byte[] key) throws IOException {
        IndexReader.Iterator i = vocabIndex.getIterator(key);
        if (i == null) {
            return null;
        } else {
            return new Iterator(i);
        }
    }

    public void close() throws IOException {
        vocabIndex.close();
        for(RandomAccessFile f : dataFiles){
          if(f != null)
            f.close();
        }
    }

    //*********************//
    // static functions
    

    public static boolean isParallelIndex(String pathname) throws IOException {
        File f = new File(pathname);

        assert f.exists() : "Path not found: " + f.getAbsolutePath();
        if (!f.isDirectory()) {
            f = f.getParentFile();
        }
        File index = new File(f.getAbsolutePath() + File.separator + "key.index");
        File data = new File(f.getAbsolutePath() + File.separator + "0");
	long magic = 0;
        if (index.exists() &&
            data.exists() &&
            IndexReader.isIndexFile(index.getAbsolutePath())) {
            RandomAccessFile reader = StreamCreator.inputStream(data.getAbsolutePath());
            reader.seek( reader.length() - 8 );
	    magic = reader.readLong();
	    reader.close();
            if(magic == VALUE_FILE_MAGIC_NUMBER){
                return true;
            }
        }
        return false;
    }
}