// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.extents;

import org.galagosearch.core.retrieval.structured.ExtentIterator;
import org.galagosearch.core.util.ExtentArray;
import java.io.IOException;

/**
 *
 * @author trevor
 */
public class FakeExtentIterator extends ExtentIterator {
    int[][] data;
    int index;

    public FakeExtentIterator( int[][] data ) {
        this.data = data;
        this.index = 0;
    }

    public void nextDocument() {
        index++;
    }

    public boolean isDone() {
        return index >= data.length;
    }

    public ExtentArray extents() {
        ExtentArray array = new ExtentArray();
        int[] datum = data[index];

        for( int i=1; i<datum.length; i++ ) {
            array.add( datum[0], datum[i], datum[i]+1 );
        }

        return array;
    }

    public int document() {
        return data[index][0];
    }

    public int count() {
        return data[index].length - 1;
    }

    public void reset() throws IOException {
        index = 0;
    }
}
