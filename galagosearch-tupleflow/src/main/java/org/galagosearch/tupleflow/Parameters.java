// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * A Parameters object is a hierarchical collection of strings.  TupleFlow
 * uses it as a convenient way to pass parameters to objects, read and write
 * parameters from files, and read parameters from the command line.
 * 
 * @author trevor
 */
public class Parameters implements Serializable {
    public static class Variable implements Serializable {
        String name;

        public Variable(String name) {
            this.name = name;
        }

        public String toString(HashMap<String, String> values) {
            return values.get(name);
        }
    }

    public static class Value implements Serializable {
        Map<String, List<Value>> _map;
        CharSequence _string;

        /**
         * Construct a new Value object with nothing in it.
         */
        public Value() {
            _map = null;
            _string = "";
        }

        /**
         * Returns true if there is no data in this value object.
         */
        public boolean isEmpty() {
            return _map == null && _string.equals("");
        }

        /**
         * Creates the map if it is currently null.
         */
        private void ensureMap() {
            if (_map == null) {
                _map = new HashMap<String, List<Value>>();
            }
        }

        /**
         * Ensures that the map exists.  Adds a values array to
         * the map if there isn't one already.  Returns the values
         * array corresponding to this key.
         */
        private List<Value> ensureKey(String key) {
            ensureMap();
            List<Value> values = new ArrayList<Value>();
            if (!_map.containsKey(key)) {
                _map.put(key, values);
            } else {
                values = _map.get(key);
            }
            return values;
        }

        /**
         * Add a new child value object.  This is similar to adding a 
         * child XML element at this point, but without actually putting any
         * data in that element yet.
         *
         * @param key The XML tag/key name of this child value.
         * @return A new empty child Value object.
         */
        public Value add(String key) {
            Value result = new Value();
            ensureKey(key).add(result);
            return result;
        }

        public void add(String key, List<Value> values) {
            if (key.contains("/")) {
                String fields[] = key.split("/", 2);
                String subKey = fields[1];
                String rootKey = fields[0];
                Value subValue = null;

                if (!containsKey(rootKey)) {
                    subValue = add(rootKey);
                } else {
                    subValue = list(rootKey).get(0);
                }
                subValue.add(subKey, values);
            } else {
                ensureKey(key).addAll(values);
            }
        }

        /**
         * Add a new XML value.  Key may be a simple tag name
         * or a slash-delimited XML pathname.
         *
         * @param key The XML path to the tag this call should modify/add.
         * @param value The text value to assign to the node specified by the key parameter.
         */
        public void add(String key, CharSequence value) {
            Value stringValue = new Value();
            stringValue._string = value;
            List<Value> valueList = Collections.singletonList(stringValue);

            add(key, valueList);
        }

        public void set(CharSequence value) {
            _map = null;
            _string = value;
        }

        public void set(String key, List<Value> values) {
            if (key.contains("/")) {
                String fields[] = key.split("/", 2);
                String subKey = fields[1];
                String rootKey = fields[0];
                Value subValue = null;

                if (!containsKey(rootKey)) {
                    subValue = add(rootKey);
                } else {
                    subValue = list(rootKey).get(0);
                }
                subValue.add(subKey, values);
            } else {
                List<Value> current = ensureKey(key);
                current.clear();
                current.addAll(values);
            }
        }

        @Override
        public String toString() {
            return _string.toString();
        }

        public Map<String, List<Value>> map() {
            return _map;
        }

        public String get(String key, String def) {
            if (containsKey(key)) {
                return get(key);
            }
            return def;
        }

        public String get(String key) {
            if (key == null || key.length() == 0) {
                return toString();
            }
            List<Value> list = list(key);

            if (list == null) {
                throw new IllegalArgumentException("Key '" + key + "' not found.");
            }
            Value first = list.get(0);
            return first.toString();
        }

        public List<Value> list(String key) {
            // this key may actually be a path expression.
            // if so, we consider just the first part, and call that the key
            String[] fields = key.split("/", 2);
            key = fields[0];

            if (_map == null) {
                return Collections.emptyList();
            }
            
            // get the appropriate list from the map
            List<Value> list = _map.get(key);

            // if it's not a path, just return what we found.
            if (fields.length == 1) {
                return list;
            } else {
                // it's a path, so we descend through the first
                // item of this list, then ask for the list corresponding
                // to the rest of this path
                String tail = fields[1];
                return list.get(0).list(tail);
            }
        }

        public List<String> stringList(String key) {
            List<Value> list = list(key);
            ArrayList<String> strings = new ArrayList<String>(list.size());

            for (Value value : list) {
                strings.add(value.toString());
            }

            return strings;
        }

        public boolean containsKey(String key) {
            try {
                get(key);
            } catch (Exception e) {
                return false;
            }

            return true;
        }
    }
    Value _data = new Value();
    HashMap<String, String> _variables = new HashMap<String, String>();

    /**
     * This class gathers up a stack of CharSequence objects and makes
     * them look like a single CharSequence.  The reason we do this is so
     * that we can insert special, mutable CharSequences in here that are used
     * as parameters.
     * 
     * For example, suppose we have an input string like:
     *      ${path:/Users/trevor/Desktop}.txt
     * We can make a CharSequenceBuffer = [ MutableCharSequence("path"), ".txt" ]
     * 
     * Now, we can go and change the MutableCharSequence later so that parameters work
     * appropriately.
     */
    public class CharSequenceBuffer implements CharSequence {
        ArrayList sequences = new ArrayList();

        public void add(Object sequence) {
            sequences.add(sequence);
        }

        public boolean isStatic() {
            for (Object sequence : sequences) {
                boolean isString = (sequence instanceof String);
                if (!isString) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();

            for (Object sequence : sequences) {
                builder.append(sequence.toString());
            }

            return builder.toString();
        }

        public String toString(HashMap<String, String> values) {
            StringBuilder builder = new StringBuilder();

            for (Object sequence : sequences) {
                if (sequence instanceof Variable) {
                    Variable v = (Variable) sequence;
                    builder.append(_variables.get(v.name));
                } else {
                    builder.append(sequence.toString());
                }
            }

            return builder.toString();
        }

        public int length() {
            return toString().length();
        }

        public char charAt(int index) {
            return toString().charAt(index);
        }

        public CharSequence subSequence(int start, int end) {
            return toString().subSequence(start, end);
        }
    }

    public class Parser extends DefaultHandler {
        CharSequenceBuffer writer = new CharSequenceBuffer();
        Stack<Value> contexts = new Stack();
        Value current;

        @Override
        public void characters(char[] data, int start, int length) throws SAXException {
            writer.add(new String(data, start, length));
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            if (qName.equals("parameters") && current == null) {
                contexts.push(_data);
                current = _data;
            } else if (current == null) {
                throw new SAXException("Found an outermost tag that was not 'parameters': " + qName);
            } else if (qName.equals("variable")) {
                String variableName = attributes.getValue("name");
                String variableDefault = attributes.getValue("default");
                Variable variable = new Variable(variableName);

                _variables.put(variableName, variableDefault);
                writer.add(variable);
            } else {
                writer = new CharSequenceBuffer();
                current = current.add(qName);
                contexts.push(current);
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            // if there are no variables in there, store as a String
            if (current.isEmpty()) {
                if (writer.isStatic()) {
                    current.set(writer.toString());
                } else {
                    current.set(writer);            // make a new sequence
                }
            }
            writer = new CharSequenceBuffer();
            contexts.pop();

            if (!contexts.empty()) {
                current = contexts.peek();
            } else {
                current = null;
            }
        }
    }

    public Parameters() {
    }

    /**
     * Creates a Parameters object using XML data.
     * 
     * @param xmlData
     * @throws java.io.IOException
     */
    
    public Parameters(byte[] xmlData) throws IOException {
        parse(xmlData);
    }

    /**
     * Creates a Parameters object using the contents of an XML file.
     * 
     * @param f The file to grab XML data from.
     * @throws java.io.IOException
     */
    public Parameters(File f) throws IOException {
        parse(f.getPath());
    }

    public Parameters(Value v) {
        _data = v;
    }

    /**
     * Creates a Parameters object based on a key/value map.
     * 
     * @param map
     */
    public Parameters(Map<String, String> map) {
        _data = new Value();

        for (String key : map.keySet()) {
            _data.add(key, map.get(key));
        }
    }

    /**
     * <p>
     * Fills in a Parameters object based on command line flags.  The
     * flag format is like this:
     * </p>
     * 
     * <pre>
     * --a.b.c=d
     * </pre>
     * 
     * <p>
     * This is equivalent to:
     * <pre>
     * &gt;a&lt;&gt;b&lt;&gt;c&ltd&gt;/c&lt;&gt;/b&lt;&gt;/a&lt;
     * </pre>
     * </p>
     * 
     * <p>A flag has no equals sign, like this one:</p>
     * <pre>
     * --a.b.c
     * </pre>
     * <p>is equivalent to:</p>
     * <pre>
     * --a.b.c=True
     * </pre>
     * 
     * <p>Any argument that doesn't begin with a dash is assumed to be the filename
     * of an XML file.  The data from that file data will be added to this object.</p>
     * 
     * @param args
     * @throws java.io.IOException
     */
    public Parameters(String[] args) throws IOException {
        for (String arg : args) {
            if (arg.startsWith("-")) {
                // this is a command-line argument, not a parameters file
                int startIndex = 1;

                // skip any number of leading dashes
                while (arg.length() > startIndex && arg.charAt(startIndex) == '-') {
                    startIndex++;                // split on equals (format is --argument=value, or just --argument)
                }
                String[] fields = arg.substring(startIndex).split("=");
                // on the command line, we allow either slashes or dots as the key;
                // like --corpus/path=collection or --corpus.path=collection,
                // but internal code requires slashes.
                String key = fields[0].replace('.', '/');
                String value;

                // if there's no explicit value, assume they just mean 'true'
                if (fields.length == 1) {
                    value = "True";
                } else {
                    value = fields[1];
                }

                _data.add(key, value);
            } else {
                // it's a file, so parse it
                parse(arg);
            }
        }
    }

    public Parser getParseHandler() {
        return new Parser();
    }

    /**
     * Gets the value for this key.  You can retrieve a nested value
     * using a path syntax, e.g. get("a/b/c").
     * 
     * @param key
     */
    
    public String get(String key) {
        return _data.get(key);
    }
    
    /**
     * Gets the value for this key.  Returns def if key isn't found.
     * 
     * @param key
     * @param def
     */

    public String get(String key, String def) {
        try {
            return get(key);
        } catch (Exception e) {
            return def;
        }
    }
    
    /**
     * Gets the value for key.  If key is not found, returns the
     * value for "default".  If "default" isn't found, returns def.
     * 
     * @param key
     * @param def
     * @return
     */

    public String getAsDefault(String key, String def) {
        return get(get(key), get("default", def));
    }

    /**
     * Gets the value for this key, but returning a default value if the
     * key doesn't exist in the object.  This method tries to convert the
     * value to boolean.  Values starting with 'T', 'Y', or a non-zero number
     * are considered to be true; everything else is false.
     * 
     * @param key
     * @param def
     * @return
     */
    public boolean get(String key, boolean def) {
        try {
            String result = get(key);
            char c = result.charAt(0);

            // True, true, Yes, yes, non-zero
            if (c == 'T' || c == 't' || c == 'Y' || c == 'y' || (Character.isDigit(c) && c != '0')) {
                return true;
            }
            return false;
        } catch (Exception e) {
            return def;
        }
    }

    public boolean getAsDefault(String key, boolean def) {
        return get(key, get("default", def));
    }

    public long get(String key, long def) {
        try {
            String result = get(key);
            return Long.parseLong(result);
        } catch (Exception e) {
            return def;
        }
    }

    public long getAsDefault(String key, long def) {
        return get(key, get("default", def));
    }

    public double get(String key, double def) {
        try {
            String result = get(key);
            return Double.parseDouble(result);
        } catch (Exception e) {
            return def;
        }
    }

    public double getAsDefault(String key, double def) {
        return get(get(key), get("default", def));
    }

    public void copy(String key, Parameters other) {
        List<Value> values = other.list(key);

        if (values == null) {
            return;
        }
        if (_data.containsKey(key)) {
            _data.list(key).addAll(values);
        } else {
            _data.add(key, values);
        }
    }

    public void copy(Parameters other) {
        if (other._data == null || other._data._map == null) {
            return;
        }
        for (String key : other._data._map.keySet()) {
            copy(key, other);
        }
    }

    @Override
    public Parameters clone() {
        Parameters p = new Parameters();
        p.copy(this);
        return p;
    }

    public void add(String key, List<Value> values) {
        _data.add(key, values);
    }

    public void add(String key, String value) {
        _data.add(key, value);
    }

    public void set(String key, List<Value> values) {
        _data.set(key, values);
    }

    public void set(String key, String value) {
        Value stringValue = new Value();
        stringValue.set(value);
        List<Value> values = Collections.singletonList(stringValue);
        _data.set(key, values);
    }

    public List<Value> list(String key) {
        if (_data != null && _data.containsKey(key)) {
            return _data.list(key);
        } else {
            return Collections.emptyList();
        }
    }

    public List<String> stringList(String key) {
        return _data.stringList(key);
    }

    public Value value() {
        return _data;
    }

    public boolean containsKey(String key) {
        return _data.containsKey(key);
    }

    public void parse(String filename) throws IOException {
        try {
            SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
            parser.parse(new File(filename), new Parser());
        } catch (Exception e) {
            throw new IOException(e.toString());
        }
    }

    public void parse(byte[] xmlData) throws IOException {
        try {
            SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
            String xmlText = new String(xmlData);
            StringReader reader = new StringReader(xmlText);
            parser.parse(new InputSource(reader), new Parser());
        } catch (Exception e) {
            throw new IOException(e.toString());
        }
    }

    public void write(Value value, Document document, Element element) {
        if (value._map == null) {
            element.appendChild(document.createTextNode(value.toString()));
        } else {
            for (String key : value._map.keySet()) {
                for (Value childValue : value._map.get(key)) {
                    Element childElement = document.createElement(key);
                    write(childValue, document, childElement);
                    element.appendChild(childElement);
                }
            }
        }
    }

    public void write(StreamResult result) throws IOException {
        try {
            Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().
                    newDocument();
            Element root = document.createElement("parameters");
            write(_data, document, root);
            document.appendChild(root);

            Transformer identity = TransformerFactory.newInstance().newTransformer();
            identity.transform(new DOMSource(document), result);
        } catch (Exception e) {
            throw new IOException(e.toString());
        }
    }

    public void write(String filename) throws IOException {
        try {
            write(new StreamResult(new File(filename)));
        } catch (Exception e) {
            throw new IOException(e.toString());
        }
    }

    @Override
    public String toString() {
        StringWriter writer = new StringWriter();
        try {
            write(new StreamResult(writer));
        } catch (Exception e) {
            return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!-- Exception: " + e.toString() + " --><parameters/>\n";
        }
        String result = writer.toString();
        return result;
    }

    public boolean isEmpty() {
        return _data.isEmpty();
    }
}
