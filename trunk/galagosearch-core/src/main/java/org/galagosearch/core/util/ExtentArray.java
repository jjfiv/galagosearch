// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.util;

import org.galagosearch.core.retrieval.structured.Extent;

/**
 * @author trevor
 */
public class ExtentArray {
    Extent[] _array;
    int _position;

    public ExtentArray(int capacity) {
        _array = new Extent[capacity];
        for(int i=0; i<_array.length; i++)
            _array[i] = new Extent();
        _position = 0;
    }

    public ExtentArray() {
        this(16);
    }

    private void makeRoomForOneObject() {
        if(_position == _array.length) {
            // grow array if we're out of space
            _array = _copyArray(_array.length * 2);
        }
    }
    
    public void add(Extent value) {
        makeRoomForOneObject();
        
        _array[_position] = value;
        _position += 1;
    }
    
    public void add(int document, int begin, int end) {
        add(document, begin, end, 1);
    }

    public void add(int document, int begin, int end, double weight) {
        makeRoomForOneObject();
        
        Extent e = _array[_position];
        e.document = document;
        e.begin = begin;
        e.end = end;
        e.weight = weight;
        _position += 1;
    }
    
    public Extent[] getBuffer() {
        return _array;
    }

    public int getPosition() {
        return _position;
    }

    private Extent[] _copyArray(int newSize) {
        Extent[] result = new Extent[newSize];
        System.arraycopy(_array, 0, result, 0, _position);
        for(int i=_position; i<result.length; i++)
            result[i] = new Extent();
        return result;
    }

    public Extent[] toArray() {
        return _copyArray(_position);
    }
    
    public void reset() {
        _position = 0;
    }
}
