/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.index.parallel;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.galagosearch.core.index.IndexElement;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.OutputClass;
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
 * @author sjh
 */
@InputClass(className = "org.galagosearch.core.types.KeyValuePair")
@OutputClass(className = "org.galagosearch.core.types.KeyValuePair")
public class ParallelIndexValueWriter implements KeyValuePair.KeyOrder.ShreddedProcessor,
    Source<KeyValuePair> {

    public Processor<KeyValuePair> processor;

    int valueOutputId;
    DataOutputStream valueOutput;

    byte[] lastKey = null;
    ByteArrayOutputStream keyArray;
    DataOutputStream keyStream;
    long valueOffset;
    long valueLength;

    public ParallelIndexValueWriter(TupleFlowParameters parameters) throws IOException{
        String valueOutputPath = parameters.getXML().get("filename") + File.separator + parameters.getInstanceId();
        Utility.makeParentDirectories( valueOutputPath );

        valueOutputId = parameters.getInstanceId();
        valueOutput = StreamCreator.realOutputStream( valueOutputPath );
        
        valueOffset = 0;
    }

    public void add(IndexElement list) throws IOException {
        processKey(list.key());
        valueOffset += list.dataLength();
        valueLength += list.dataLength();
        list.write( valueOutput );
    }

    public void processKey(byte[] key) throws IOException {

        if(lastKey != null){
            keyStream.writeLong( valueLength ); // value length
            keyStream.close();
            processor.process( new KeyValuePair( lastKey, keyArray.toByteArray() ) );
        }

        lastKey = key;
        keyArray = new ByteArrayOutputStream();
        keyStream = new DataOutputStream(keyArray);

        keyStream.writeInt( valueOutputId ); // file
        keyStream.writeLong( valueOffset ); //valueOffset

        valueLength = 0;
    }

    public void processTuple(byte[] value) throws IOException {
        valueOutput.write(value);
        valueLength += value.length;
        valueOffset += value.length;
    }

    public void close() throws IOException {
        if(lastKey != null){
            keyStream.writeLong( valueLength ); // value length
            keyStream.close();
            processor.process( new KeyValuePair( lastKey, keyArray.toByteArray() ) );
        }
        valueOutput.close();

        processor.close();
    }

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
