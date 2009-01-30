// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 *
 * @author trevor
 */
public class VocabularyWriter {
    DataOutputStream output;
    ByteArrayOutputStream buffer;

    public VocabularyWriter() throws IOException {
        buffer = new ByteArrayOutputStream();
        output = new DataOutputStream(new BufferedOutputStream(buffer));
    }

    public void add(byte[] word, long offset) throws IOException {
        output.writeShort(word.length);
        output.write(word);
        output.writeLong(offset);
    }

    public byte[] data() throws IOException {
        output.close();
        return buffer.toByteArray();
    }
}

