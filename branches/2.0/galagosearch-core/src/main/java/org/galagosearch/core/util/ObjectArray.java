// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.util;

public class ObjectArray<T> {
  Object[] _array;
  int _position;

  public ObjectArray(int capacity) {
    _array = new Object[capacity];
    _position = 0;
  }

  public ObjectArray() {
    this(16);
  }

  public void add(T value) {
    if(_position == _array.length) {
      // grow array if we're out of space
      _array = _copyArray(_array.length * 2);
    }

    _array[_position] = value;
    _position += 1;
  }

  public T[] getBuffer() {
    return (T[]) _array;
  }

  public int getPosition() {
    return _position;
  }

  private Object[] _copyArray(int newSize) {
    Object[] result = new Object[newSize];
    System.arraycopy(_array, 0, result, 0, _position);
    return result;
  }

  public T[] toArray() {
    return (T[]) _copyArray(_position);
  }
}
