/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
public abstract class AbstractModifier implements StructuredIndexPartModifier {

  protected GenericIndexReader reader;
  protected String source;
  protected String name;

  public AbstractModifier(GenericIndexReader reader) {
    this.reader = reader;
    Parameters p = reader.getManifest();
    source = p.get("part", "unknown");
    name = p.get("name", "unknown");
  }

  public AbstractModifier(String filename) throws FileNotFoundException, IOException {
    this(GenericIndexReader.getIndexReader(filename));
  }

  public void close() throws IOException {
    this.reader.close();
  }

  public String getSourcePart() {
    return source;
  }

  public String getModifierName() {
    return name;
  }

  public static String getModifierName(String dir, String part, String name) {
      return String.format("%s/mod/%s.%s", dir, part, name);
  }

  public Parameters getManifest() {
    return reader.getManifest();
  }

  public boolean isEligible(Node node) {
    Parameters p = node.getParameters();
    return (p.get("part", "none").equals(source) &&
            p.get("mod", "none").equals(name));
  }
}