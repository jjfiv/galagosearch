// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class PositionalByteStream extends InputStream {
    int pos = 0;
    InputStream input;
    
    public PositionalByteStream(InputStream in) {
        this.input = in;
        this.pos = 0;
    }
    
    @Override
    public int read(byte[] buffer) throws IOException {
        int result = input.read(buffer);
        if(result > 0)
            pos += result;
        return result;
    }

    public int read() throws IOException {
        int result = input.read();
        if(result >= 0)
            pos += 1;
        else
            throw new EOFException("Read off the end of the stream.");
        return result;
    }

    public int position() {
        return pos;
    }
}
