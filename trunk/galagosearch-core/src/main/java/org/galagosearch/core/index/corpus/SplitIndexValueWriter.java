/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.index.corpus;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.galagosearch.core.index.GenericIndexWriter;
import org.galagosearch.core.index.IndexElement;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Source;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.StreamCreator;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 *
 * Split index value writer
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
@InputClass(className = "org.galagosearch.core.types.KeyValuePair")
@OutputClass(className = "org.galagosearch.core.types.KeyValuePair")
public class SplitIndexValueWriter extends GenericIndexWriter
        implements KeyValuePair.KeyValueOrder.ShreddedProcessor {

    public static final long MAGIC_NUMBER = 0x2b3c4d5e6f7a8b9cL;
    public Processor<KeyValuePair> processor;
    public Parameters manifest;
    private int valueOutputId;
    private DataOutputStream valueOutput;
    private byte[] lastKey = null;
    private ByteArrayOutputStream keyArray;
    private DataOutputStream keyStream;
    private long valueOffset;
    private long valueLength;
    private short valueBlockSize;
    private Counter docCounter;
    
    public SplitIndexValueWriter(TupleFlowParameters parameters) throws IOException {
        String valueOutputPath = parameters.getXML().get("filename") + File.separator + parameters.getInstanceId();
        Utility.makeParentDirectories(valueOutputPath);
	docCounter = parameters.getCounter("Document Values Stored");
        manifest = parameters.getXML();

        valueOutputId = parameters.getInstanceId();
        valueOutput = StreamCreator.realOutputStream(valueOutputPath);
        valueOffset = 0;

        if (parameters.getXML().get("blockIndex", false)) {
            valueBlockSize = (short) parameters.getXML().get("valueBlockSize", 32768);
        } else {
            valueBlockSize = 0;
        }
    }

    public Parameters getManifest() {
        return manifest;
    }

    public void add(IndexElement list) throws IOException {
        processKey(list.key());
        valueOffset += list.dataLength();
        valueLength += list.dataLength();
        list.write(valueOutput);
    }

    @Override
    public long getValueBlockSize() {
        return manifest.get("valueBlockSize", valueBlockSize);
    }

    @Override
    public void processKey(byte[] key) throws IOException {

        if (lastKey != null) {
            keyStream.writeLong(valueLength); // value length
            keyStream.close();
            processor.process(new KeyValuePair(lastKey, keyArray.toByteArray()));
	    if (docCounter != null) docCounter.increment();
        }

        lastKey = key;
        keyArray = new ByteArrayOutputStream();
        keyStream = new DataOutputStream(keyArray);

        keyStream.writeInt(valueOutputId); // file
        keyStream.writeLong(valueOffset); //valueOffset

        valueLength = 0;
    }

    @Override
    /**
     * TODO: This needs to be changed to use blocks properly
     *  - currently it's identical to the add function above
     */
    public void processValue(byte[] value) throws IOException {
        valueOutput.write(value);
        valueLength += value.length;
        valueOffset += value.length;
    }

    public void processTuple() throws IOException{
        // nothing //
    }

    public void close() throws IOException {
        if (lastKey != null) {
            keyStream.writeLong(valueLength); // value length
            keyStream.close();
            processor.process(new KeyValuePair(lastKey, keyArray.toByteArray()));
        }

        // write the value block size
        valueOutput.writeShort(valueBlockSize);
        // write the magic number.
        valueOutput.writeLong(MAGIC_NUMBER);
        valueOutput.close();
        processor.close();
    }


    @Override
    public void setProcessor(Step next) throws IncompatibleProcessorException {
        Linkage.link(this, next);
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        if (!parameters.getXML().containsKey("filename")) {
            handler.addError("DocumentIndexWriter requires an 'filename' parameter.");
            return;
        }

        String index = parameters.getXML().get("filename");
        Verification.requireWriteableDirectory(index, handler);
    }
}
