/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.parse;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.util.StreamReaderDelegate;

/*
 *
 * @author marc
 */
public class WikiParser implements DocumentStreamParser {

  StreamReaderDelegate reader;
  XMLInputFactory factory;

  /** Creates a new instance of TrecWebParser */
  public WikiParser(BufferedReader reader) throws FileNotFoundException, IOException {

    factory = XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.IS_COALESCING, true);
    try {
      this.reader = new StreamReaderDelegate(factory.createXMLStreamReader(reader));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public Document nextDocument() throws IOException {
    if (reader == null) {
      return null;
    }

    Document doc = null;
    String id = null;
    StringBuilder sb;
    try {
      // create a new document
      while (reader.hasNext()) {

        if (reader.next() == XMLStreamConstants.END_ELEMENT) {
          String name = reader.getLocalName();
          if (name.equals("page") && doc != null) {
            return doc;
          }
        }

        if (reader.next() == XMLStreamConstants.START_ELEMENT) {
          String name = reader.getLocalName();
          if (name.equals("id") && id == null) {
            id = reader.getElementText();
          }

          if (name.equals("text")) {
            doc = new Document();
            doc.identifier = id;
            doc.text = reader.getElementText();
          }
        }
      }
      return null;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
