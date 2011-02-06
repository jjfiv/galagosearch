/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.index.parallel;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.core.index.VocabularyReader;
import org.galagosearch.tupleflow.BufferedFileDataStream;
import org.galagosearch.tupleflow.DataStream;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.StreamCreator;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class ParallelIndexReader {

    IndexReader vocabIndex;
    String indexFolder;
    int hashMod;

    public class Iterator {

        IndexReader.Iterator vocabIterator;
        RandomAccessFile[] dataFiles = new RandomAccessFile[hashMod];
        boolean valueLoaded = false;
        int file;
        long valueOffset;
        long valueLength;

        public Iterator(IndexReader.Iterator vocabIterator) {
            this.vocabIterator = vocabIterator;
        }

        public void skipTo(byte[] key) throws IOException {
            vocabIterator.skipTo(key);
            valueLoaded = false;
        }

        /**
         * Advances to the next key in the index.
         *
         * @return true if the advance was successful, false if no more keys remain.
         * @throws java.io.IOException
         */
        public boolean nextKey() throws IOException {
            valueLoaded = false;
            return vocabIterator.nextKey();
        }

        public RandomAccessFile getInput() throws IOException {
            if (!valueLoaded) {
                loadValue();
            }
            return dataFiles[file];
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
         * Returns the key associated with the current inverted list.
         */
        public byte[] getKey() {
            return vocabIterator.getKey();
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
         * Returns the value as a string.
         */
        public String getValueString() throws IOException {
            byte[] data = getValueBytes();
            return Utility.toString(data);

        }

        /**
         * Returns the value as a string.
         */
        public byte[] getValueBytes() throws IOException {
            DataStream stream = getValueStream();
            assert stream.length() < Integer.MAX_VALUE;
            byte[] data = new byte[(int) stream.length()];
            stream.readFully(data);
            return data;
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

        /**
         * Reads the header information for a data value
         *
         * @throws IOException
         */
        void loadValue() throws IOException {
            valueLoaded = true;
            DataStream stream = vocabIterator.getValueStream();

            file = stream.readInt();
            valueOffset = stream.readLong();
            valueLength = stream.readLong();

            if (dataFiles[file] == null) {
                dataFiles[file] = StreamCreator.inputStream(indexFolder + File.separator + file);
            }
            dataFiles[file].seek(valueOffset);
        }
    }

    public ParallelIndexReader(String filename) throws IOException {
        File f = new File(filename);
        if (f.isDirectory()) {
            f = new File(filename + File.separator + "key.index");
        }
        vocabIndex = new IndexReader(f);

        indexFolder = f.getParent();
        //  (-1) for the key index
        hashMod = f.getParentFile().list().length - 1;
    }

    public static boolean isParallelIndex(String pathname) throws IOException {
        File f = new File(pathname);

        assert f.exists() : "Path not found: " + f.getAbsolutePath();

        if (!f.isDirectory()) {
            f = f.getParentFile();
        }

        File index = new File(f.getAbsolutePath() + File.separator + "key.index");
        File data = new File(f.getAbsolutePath() + File.separator + "0");
        if (index.exists() &&
            data.exists() &&
            IndexReader.isIndexFile(index.getAbsolutePath())) {

            return true;
        }
        return false;
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
        Iterator i = new Iterator(vocabIndex.getIterator(key));
        if (i == null) {
            return null;
        } else {
            return i;
        }
    }

    /**
     * Gets the value stored in the index associated with this key.
     * @param key
     * @return The index value for this key, or null if there is no such value.
     * @throws java.io.IOException
     */
    public String getValueString(byte[] key) throws IOException {
        Iterator iter = getIterator(key);

        if (iter == null) {
            return null;
        }
        return iter.getValueString();
    }

    /**
     * Gets the value stored in the index associated with this key.
     * @param key
     * @return The index value for this key, or null if there is no such value.
     * @throws java.io.IOException
     */
    public byte[] getValueBytes(byte[] key) throws IOException {
        Iterator iter = getIterator(key);

        if (iter == null) {
            return null;
        }
        return iter.getValueBytes();
    }

    /**
     * Gets the value stored in the index associated with this key.
     *
     * @param key
     * @return The index value for this key, or null if there is no such value.
     * @throws java.io.IOException
     */
    public DataStream getValueStream(byte[] key) throws IOException {
        Iterator iter = getIterator(key);

        if (iter == null) {
            return null;
        }
        return iter.getValueStream();
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

    public void close() throws IOException {
        vocabIndex.close();
    }

    /**
     *
     * TESTING FUNCTION -- TO BE DELETED
     */
    public static void main(String[] args) throws Exception {
        String folder = args[0];
        System.err.println(folder);

        ParallelIndexReader reader = new ParallelIndexReader(folder);
        Iterator i = reader.getIterator();
        while (!i.isDone()) {
            String key = Utility.toString(i.getKey());
            // manual forced loading
            i.loadValue();

            System.err.println(key + "\t" + i.file + "\t" + i.valueOffset + "\t" + i.valueLength);
            i.nextKey();
        }
    }
}
