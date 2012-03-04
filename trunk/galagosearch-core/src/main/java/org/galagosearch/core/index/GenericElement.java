/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.index;

import java.io.IOException;
import java.io.OutputStream;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class GenericElement implements IndexElement {
    byte[] key;
    byte[] data;

    public GenericElement(byte[] key, byte[] data) {
        this.key = key;
        this.data = data;
    }
    
    public GenericElement(String key, byte[] data) {
        this.key = Utility.makeBytes(key);
        this.data = data;
    }
    
    public GenericElement(String key, String value) {
        this.key = Utility.makeBytes(key);
        this.data = Utility.makeBytes(value);
    }

    public byte[] key() {
        return key;
    }

    public long dataLength() {
        return data.length;
    }

    public void write(OutputStream stream) throws IOException {
        stream.write(data);
    }
}