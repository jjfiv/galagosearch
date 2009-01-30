// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow.execution;

import java.io.Serializable;
import org.xml.sax.Locator;

/**
 *
 * @author trevor
 */
public class FileLocation implements Serializable, Comparable<FileLocation> {
    public FileLocation(String fileName, int lineNumber, int columnNumber) {
        this.fileName = fileName;
        this.lineNumber = lineNumber;
        this.columnNumber = columnNumber;
    }

    public FileLocation(String filename, Locator locator) {
        this(filename, locator.getLineNumber(), locator.getColumnNumber());
    }

    public int compareTo(FileLocation location) {
        int result = fileName.compareTo(location.fileName);
        if (result == 0) {
            result = lineNumber - location.lineNumber;
        }
        if (result == 0) {
            result = columnNumber - location.columnNumber;
        }
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s [Line %d: Column %d]", fileName, lineNumber, columnNumber);
    }

    public String fileName;
    public int lineNumber;
    public int columnNumber;
}
