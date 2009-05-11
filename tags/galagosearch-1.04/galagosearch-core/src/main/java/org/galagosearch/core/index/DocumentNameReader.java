// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import org.galagosearch.tupleflow.Utility;
/**
 * Reads a binary file of document names produced by DocumentNameWriter.
 * The names are loaded into RAM for quick access.
 *
 * @author trevor
 */
public class DocumentNameReader {
    private static class NameSlot {
        public String prefix;
        public int offset;
        public int footerWidth;
        public int[] footers;
    }

    ArrayList<NameSlot> slots;
    int documentCount;
    
    /** Creates a new instance of DocumentNameReader */
    public DocumentNameReader(String filename) throws IOException {
        FileInputStream f = new FileInputStream(filename);
        DataInputStream input = new DataInputStream(new BufferedInputStream(f));
        slots = new ArrayList();
        read(input);
        input.close();
    }
    
    private String getInSlot(NameSlot slot, int footerIndex) {
        int footer = slot.footers[footerIndex-slot.offset];
        String prefix = slot.prefix;
        String documentName;
        
        if(slot.footerWidth == 0) {
            documentName = slot.prefix;
        } else {
            String format = "%s-%0" + slot.footerWidth + "d";
            documentName = String.format(format, prefix, footer);
        }
        
        return documentName;
    }
    
    public String get(int index) {
        assert index >= 0;
        assert index < documentCount;
        
        if(index >= documentCount) 
            return "unknown";
        
        if(index < 0)
            return "unknown";
        
        int big = slots.size()-1;
        int small = 0;
        
        while(big-small > 1) {
            int middle = small + (big-small)/2;
            
            if(slots.get(middle).offset >= index)
                big = middle;
            else
                small = middle;
        }

        NameSlot one = slots.get(small);
        NameSlot two = slots.get(big);
        String result = "";
        
        if (two.offset <= index)
            result = getInSlot(two, index);
        else
            result = getInSlot(one, index);
        
        return result;
    }
    
    public void read(DataInputStream input) throws IOException {
        int offset = 0;
        
        // open a file
        while(input.available() > 0) {
            // read the prefix
            int prefixLength = input.readInt();
            byte[] prefixData = new byte[prefixLength];
            input.read(prefixData);
            
            // read the footers
            int footerWidth = input.readInt();
            int footerCount = input.readInt();
            NameSlot slot = new NameSlot();
            
            slot.footerWidth = footerWidth;
            slot.offset = offset;
            slot.prefix = Utility.makeString(prefixData);
            slot.footers = new int[footerCount];
            
            for(int i=0; i<footerCount; i++) {
                slot.footers[i] = input.readInt();
            }
            
            slots.add(slot);
            offset += slot.footers.length;
        }
        
        documentCount = offset;
    }
}
