// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.zip.GZIPInputStream;
import org.galagosearch.core.index.IndexWriter;
import org.galagosearch.core.index.VocabularyReader.TermSlot;
import org.galagosearch.core.index.VocabularyReader;
import org.galagosearch.tupleflow.BufferedFileDataStream;
import org.galagosearch.tupleflow.DataStream;
import org.galagosearch.tupleflow.MemoryDataStream;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 * <p>This implements the core functionality for all inverted list readers.  It can
 * also be used as a read-only TreeMap for disk-based data structures.  In Galago,
 * it is used both to store index data and to store documents.</p>
 * 
 * <p>An index is a mapping from String to byte[].  If compression is turned on, the
 * value must be small enough that it fits in memory.  If compression is off, values
 * are streamed directly from disk so there is no size restriction.  Indexes support
 * iteration over all keys, or direct lookup of a single key.  The structure is optimized
 * to support fast random lookup on disks.</p>
 * 
 * <p>Data is stored in blocks, typically 32K each.  Each block has a prefix-compressed
 * set of keys at the beginning, followed by a block of value data.  IndexWriter/IndexReader
 * can GZip compress that value data, or it can be stored uncompressed.  For inverted list
 * data it's best to use your own compression, but for text data the GZip compression
 * is a good choice.</p>
 * 
 * <p>Typically this class is extended by composition instead of inheritance.</p>
 * 
 * @author trevor
 */
public class IndexReader {
    VocabularyReader vocabulary;
    RandomAccessFile input;
    Parameters manifest;
    int blockSize = 65536;
    int vocabGroup = 16;
    long vocabularyOffset;
    long manifestOffset;
    long footerOffset;
    boolean isCompressed;
    
    private static class VocabularyBlock {
        long startFileOffset;
        long startValueFileOffset;
        long endValueFileOffset;

        long[] endValueOffsets;
        String[] keys;

        public VocabularyBlock(
                long startFileOffset,
                long startValueFileOffset,
                long endValueFileOffset,
                long[] endValueOffsets, String[] keys) {
            this.keys = keys;
            this.endValueOffsets = endValueOffsets;
            this.startFileOffset = startFileOffset;
            this.startValueFileOffset = startValueFileOffset;
            this.endValueFileOffset = endValueFileOffset;
        }
        
        public long getValuesStart() {
            return startValueFileOffset;
        }
        
        public long getValuesEnd() {
            return endValueFileOffset;
        }

        public long getBlockEnd() {
            return getValuesEnd();
        }

        public long getListStart(int index) {
            if (index == 0) {
                return startValueFileOffset;
            }
            return endValueFileOffset - endValueOffsets[index - 1];
        }

        public long getListEnd(int index) {
            return endValueFileOffset - endValueOffsets[index];
        }
        
        public long getUncompressedEndOffset(int index) {
            return endValueOffsets[index];
        }

        public boolean hasMore(int index) {
            return endValueOffsets.length > (index + 1);
        }

        public int findIndex(String term) {
            // Need to do a linear search here because the vocabulary order
            // does not necessarily match Java's idea of alphabetical order
            for (int i = 0; i < keys.length; i++) {
                if (keys[i].equals(term)) {
                    return i;
                }
            }
            return -1;
        }

        private String getKey(int termIndex) {
            return this.keys[termIndex];
        }
    }

    public class Iterator {
        VocabularyBlock block;
        byte[] decompressedData;
        String key;
        int keyIndex;
        boolean done;

        Iterator(VocabularyBlock block, int index) throws IOException {
            this.block = block;
            keyIndex = index;
            done = false;

            loadIndex();
            decompressedData = null;
        }

        void loadIndex() throws IOException {
            key = "";

            if (block == null || keyIndex < 0) {
                done = true;
                return;
            }
            key = block.getKey(keyIndex);
        }

        void invalidateBlock() {
            decompressedData = null;
        }
        
        void decompressBlock() throws IOException {
            int blockLength = (int) (block.getValuesEnd() - block.getValuesStart());
            byte[] data = new byte[blockLength];
            input.seek(block.getValuesStart());
            input.readFully(data);
            
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            DataInputStream dataIn = new DataInputStream(in);
            int uncompressedLength = dataIn.readInt();
            
            GZIPInputStream stream = new GZIPInputStream(in);
            decompressedData = new byte[uncompressedLength];
            int totalRead = 0;
            while (totalRead < uncompressedLength) {
                int remaining = decompressedData.length - totalRead;
                int bytesRead = stream.read(decompressedData, totalRead, remaining);
                if (bytesRead <= 0) {
                    throw new EOFException("Too little data was found.");
                }
                totalRead += bytesRead;
            }
        }
        
        public void skipTo(byte[] key) throws IOException {
            TermSlot slot = vocabulary.get(key);
            if (slot == null) {
                done = true;
            } else {
                invalidateBlock();
                block = readVocabularyBlock(slot.begin);
                keyIndex = 0;
                while (keyIndex < block.keys.length) {
                    byte[] blockKey = Utility.makeBytes(block.keys[keyIndex]);
                    if (Utility.compare(key, blockKey) <= 0) {
                        loadIndex();
                        return;
                    }
                    keyIndex++;
                }
                done = true;
            }
        }

        /**
         * Returns true if no more keys remain to be read.
         */
        public boolean isDone() {
            return done;
        }
        
        /**
         * Returns the byte offset of the end of this block.
         */
        public long getFilePosition() {
            return block.getBlockEnd();
        }
        
        /**
         * Returns the key associated with the current inverted list.
         */
        public String getKey() {
            return key;
        }
        
        /**
         * Returns the value as a buffered stream.
         */
        public DataStream getValueStream() throws IOException {
            if (isCompressed) {
                // Lazy decompression allows fast scans over the key space
                // of a table without decompressing all the values.
                if (decompressedData == null) {
                    decompressBlock();
                }

                int start = 0;
                int end = (int) (decompressedData.length - block.getUncompressedEndOffset(keyIndex));
                if (keyIndex > 0) {
                    start = (int) (decompressedData.length - block.getUncompressedEndOffset(keyIndex - 1));
                }
                int length = end - start;
                
                return new MemoryDataStream(decompressedData, start, length);
            } else {
                return blockStream(this);
            }
        }
        
        /**
         * Returns the length of the value, in bytes.
         */
        public long getValueLength() throws IOException {
            return getValueStream().length();
        }

        /**
         * Returns the value as a string.
         */
        
        public String getValueString() throws IOException {
            DataStream stream = getValueStream();
            assert stream.length() < Integer.MAX_VALUE;
            byte[] data = new byte[(int) stream.length()];
            stream.readFully(data);
            return Utility.makeString(data);
        }
        
        /**
         * Returns the byte offset
         * of the beginning of the current inverted list,
         * relative to the start of the whole inverted file.
         */
        public long getValueStart() {
            return block.getListStart(keyIndex);
        }
        
        public long getDataStart() {
            if (isCompressed) return -1;
            return block.getListStart(keyIndex);
        }
        
        public long getDataEnd() {
            if (isCompressed) return -1;
            return block.getListEnd(keyIndex);
        }

        /**
         * Returns the byte offset
         * of the end of the current inverted list,
         * relative to the start of the whole inverted file.
         */
        public long getValueEnd() {
            return block.getListEnd(keyIndex);
        }

        /**
         * Advances to the next key in the index.
         * 
         * @return true if the advance was successful, false if no more keys remain.
         * @throws java.io.IOException
         */
        public boolean nextKey() throws IOException {
            if (block.hasMore(keyIndex)) {
                keyIndex++;
            } else if (block.getBlockEnd() >= IndexReader.this.vocabularyOffset) {
                invalidateBlock();
                done = true;
                return false;
            } else {
                invalidateBlock();
                block = readVocabularyBlock(block.getBlockEnd());
                keyIndex = 0;
            }

            loadIndex();
            return true;
        }
    }

    /**
     * Opens an index found in the at pathname.
     * 
     * @param pathname Filename of the index to open.
     * @throws FileNotFoundException
     * @throws IOException
     */
    public IndexReader(String pathname) throws FileNotFoundException, IOException {
        input = new RandomAccessFile(pathname, "r");

        // Seek to the end of the file
        long length = input.length();
        footerOffset = length - 2*Integer.SIZE/8 - 3*Long.SIZE/8 - 1; 
        input.seek(footerOffset);
        
        // Now, read metadata values:
        vocabularyOffset = input.readLong();
        manifestOffset = input.readLong();
        blockSize = input.readInt();
        vocabGroup = input.readInt();
        isCompressed = input.readBoolean();
        long magicNumber = input.readLong();
        
        if (magicNumber != IndexWriter.MAGIC_NUMBER) {
            throw new IOException("This does not appear to be an index file (wrong magic number)");
        }
        long invertedListLength = vocabularyOffset;
        long vocabularyLength = manifestOffset - vocabularyOffset;
        
        input.seek(vocabularyOffset);
        vocabulary = new VocabularyReader(input, invertedListLength, vocabularyLength);
        
        input.seek(manifestOffset);
        byte[] xmlData = new byte[(int) (footerOffset - manifestOffset)];
        input.read(xmlData);
        manifest = new Parameters(xmlData);
    }

    /**
     * Returns true if the file specified by this pathname was probably written by IndexWriter.
     * If this method returns false, the file is definitely not readable by IndexReader.
     * 
     * @param pathname
     * @return
     * @throws java.io.FileNotFoundException
     * @throws java.io.IOException
     */
    public static boolean isIndexFile(String pathname) throws FileNotFoundException, IOException {
        RandomAccessFile f = new RandomAccessFile(pathname, "r");
        long length = f.length();
        long magicNumber = 0;
        
        if (length > Long.SIZE/8) {
            f.seek(length-Long.SIZE/8);
            magicNumber = f.readLong();
        }
        f.close();
        
        boolean result = (magicNumber == IndexWriter.MAGIC_NUMBER);
        return result;
    }
    
    /**
     * Identical to the {@link #IndexReader(String) other constructor}, except this
     * one takes a File object instead of a string as the parameter.
     * 
     * @param pathname
     * @throws java.io.FileNotFoundException
     * @throws java.io.IOException
     */
    public IndexReader(File pathname) throws FileNotFoundException, IOException {
        this(pathname.toString());
    }

    /**
     * Returns the vocabulary structure for this IndexReader.  Note that the vocabulary
     * contains only the first key in each block.
     */
    public VocabularyReader getVocabulary() {
        return vocabulary;
    }
    
    /**
     * Returns an iterator pointing to the very first key in the index.
     * This is typically used for iterating through the entire index,
     * which might be useful for testing and debugging tools, but probably
     * not for traditional document retrieval.
     */
    public Iterator getIterator() throws IOException {
        VocabularyBlock block = readVocabularyBlock(0);
        Iterator result = new Iterator(block, 0);
        result.loadIndex();
        return result;
    }

    /**
     * Returns an iterator pointing at a specific key.  Returns
     * null if the key is not found in the index.
     */
    public Iterator getIterator(String key) throws IOException {
        // read from offset to offset in the vocab structure (right?)
        VocabularyReader.TermSlot slot = vocabulary.get(key);

        if (slot == null) {
            return null;
        }
        VocabularyBlock block = readVocabularyBlock(slot.begin);
        int index = block.findIndex(key);

        if (index >= 0) {
            return new Iterator(block, index);
        }
        return null;
    }
    
    /**
     * Gets the value stored in the index associated with this key.
     * @param key
     * @return The index value for this key, or null if there is no such value.
     * @throws java.io.IOException
     */
    public String getValueString(String key) throws IOException {
        Iterator iter = getIterator(key);
        
        if (iter == null) {
            return null;
        }
        return iter.getValueString();
    }
    
    /**
     * Gets the value stored in the index associated with this key.
     * 
     * @param key
     * @return The index value for this key, or null if there is no such value.
     * @throws java.io.IOException
     */
    
    public DataStream getValueStream(String key) throws IOException {
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
        return manifest;
    }

    /**
     * Like the other blockStream variant, but this one uses
     * the current file location as the starting offset.
     */
    public DataStream blockStream(long len) throws IOException {
        return blockStream(input.getFilePointer(), len);
    }

    /**
     * This convenience method returns a DataStream for
     * a region of an inverted file.
     */
    public DataStream blockStream(long offset, long length) throws IOException {
        long fileLength = input.length();
        assert offset <= fileLength;
        length = Math.min(fileLength - offset, length);

        return new BufferedFileDataStream(input, offset, length + offset);
    }

    /**
     * This convenience method returns a DataStream for
     * the region of the inverted file pointed to by the iterator.
     */
    public DataStream blockStream(Iterator iter) throws IOException {
        return new BufferedFileDataStream(input, iter.getDataStart(), iter.getDataEnd());
    }

    /**
     * Returns the file object for the inverted file.  This is useful for actually
     * reading the data from a byte range returned by the iterator.
     */
    public RandomAccessFile getInput() {
        return input;
    }

    /**
     * Closes all files associated with the IndexReader.
     */
    public void close() throws IOException {
        input.close();
    }
    
    /**
     * Reads vocabulary data from a block of the inverted file.
     * 
     * The inverted file is structured into blocks which contain a little
     * bit of compressed vocabulary information followed by inverted list
     * information.  The inverted list information is application specific,
     * and is not handled by this class.  This method reads in the vocabulary
     * information for a particular block and returns it in a VocabularyBlock
     * object, which allows for quick navigation to a particular key.
     * 
     * The C++ version of this is much more efficient, because it makes no
     * attempt to decode the whole block of information.  However, for the 
     * Java version I thought that simplicity (and testability) was more
     * important than speed.  The VocabularyBlock structure helps make iteration
     * over the entire inverted file possible.
     */
    VocabularyBlock readVocabularyBlock(long slotBegin) throws IOException {
        // read in a block of data here
        DataStream blockStream = blockStream(slotBegin, blockSize);

        // now we decode everything from the stream
        long endBlock = blockStream.readLong();
        long wordCount = blockStream.readLong();

        int prefixLength = blockStream.readUnsignedByte();
        byte[] prefixBytes = new byte[prefixLength];
        blockStream.readFully(prefixBytes);

        int wordBlockCount = (int) Math.ceil((double) wordCount / vocabGroup);
        short[] wordBlockEnds = new short[wordBlockCount];
        String[] words = new String[(int) wordCount];
        long[] invertedListEnds = new long[(int) wordCount];

        for (int i = 0; i < wordBlockCount; i++) {
            wordBlockEnds[i] = blockStream.readShort();
        }

        for (int i = 0; i < wordCount; i++) {
            invertedListEnds[i] = blockStream.readShort();
        }

        for (int i = 0; i < wordCount; i += vocabGroup) {
            int suffixLength = blockStream.readUnsignedByte();
            int wordLength = suffixLength + prefixLength;
            byte[] wordBytes = new byte[wordLength];
            byte[] lastWordBytes = wordBytes;
            System.arraycopy(prefixBytes, 0, wordBytes, 0, prefixBytes.length);
            int end = (int) Math.min(wordCount, i + vocabGroup);

            blockStream.readFully(wordBytes, prefixBytes.length,
                                  wordBytes.length - prefixBytes.length);
            words[i] = Utility.makeString(wordBytes);

            for (int j = i + 1; j < end; j++) {
                int common = blockStream.readUnsignedByte();
                wordLength = blockStream.readUnsignedByte();
                assert wordLength >= 0 : "Negative word length: " + wordLength + " " + j;
                assert wordLength >= common : "word length too small: " + wordLength + " " + common + " " + j;
                wordBytes = new byte[wordLength];

                try {
                    System.arraycopy(lastWordBytes, 0, wordBytes, 0, common);
                    blockStream.readFully(wordBytes, common, wordLength - common);
                } catch (ArrayIndexOutOfBoundsException e) {
                    System.out.println("wl: " + wordLength + " c: " + common);
                    throw e;
                }
                words[j] = Utility.makeString(wordBytes);
                lastWordBytes = wordBytes;
            }
        }

        int suffixBytes = wordBlockEnds[wordBlockEnds.length - 1];
        long headerLength = 8 + // word count
                8 + // block end
                1 + prefixLength + // word prefix bytes
                2 * wordBlockCount + // word lengths 
                2 * wordCount + // inverted list endings
                suffixBytes;          // suffix storage 

        long startInvertedLists = slotBegin + headerLength;
        return new VocabularyBlock(slotBegin, startInvertedLists, endBlock, invertedListEnds, words);
    }
}
