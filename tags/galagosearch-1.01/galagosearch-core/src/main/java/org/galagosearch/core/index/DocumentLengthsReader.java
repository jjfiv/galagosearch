// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 * @author trevor
 */
public class DocumentLengthsReader {
    RandomAccessFile file;
    FileChannel channel;
    ByteBuffer buffer;
    
    public DocumentLengthsReader(String filename) throws FileNotFoundException, IOException {
        file = new RandomAccessFile(new File(filename), "r");
        channel = file.getChannel();
        buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
    }
    
    public void close() throws IOException {
        channel.close();
        file.close();
    }
    
    public int getLength(int document) {
        return buffer.getInt(document*4);
    }
}
