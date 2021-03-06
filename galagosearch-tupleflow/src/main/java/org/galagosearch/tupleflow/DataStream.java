/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.tupleflow;

import java.io.DataInput;

/**
 *
 * @author trevor
 */
public interface DataStream extends DataInput {
    DataStream subStream(long start, long length);
    long getPosition();
    boolean isDone();
    long length();

    /**
     * Seeks forward into the stream to a particular byte offset (reverse
     * seeks are not allowed).  The offset is relative to the start position of
     * this data stream, not the beginning of the file.
     */
    void seek(long offset);
}
