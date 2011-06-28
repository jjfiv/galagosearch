package org.galagosearch.core.index;

import java.io.DataInput;
import java.io.IOException;
import java.io.PrintStream;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.VByteInput;

/**
 *
 * @author irmarc
 */
public class EditDistanceReader extends AbstractModifier {

  public class EditIterator {

    int position, total;
    int limit;
    DataInput source;

    String currentTerm;
    int score;

    public EditIterator(GenericIndexReader.Iterator it, int limit) throws IOException {
      position = 0;
      source = new VByteInput(it.getInput());
      total = source.readInt();
      this.limit = limit;
      next();
    }

    public boolean isDone() {
      return (position < total && score < limit);
    }

    public boolean next() throws IOException {
      if (position >= total || score > limit) {
        return false;
      }

      score = source.readInt();
      currentTerm = source.readUTF();
      position++;
      return true;
    }

    public String getEntry() {
      return String.format("<%s, %d>", currentTerm, score);
    }

    public String getTerm() {
      return currentTerm;
    }

    public int getDistance() {
      return score;
    }
  }

  public EditDistanceReader(GenericIndexReader r) {
    super(r);
  }

  public Object getModification(Node node) throws IOException {
    String term = node.getDefaultParameter();
    int l = (int) node.getParameters().get("distance", Integer.MAX_VALUE);
    return new EditIterator(reader.getIterator(Utility.fromString(term)), l);
  }

  public void printContents(PrintStream out) throws IOException {
    GenericIndexReader.Iterator iterator = reader.getIterator();

    while (!iterator.isDone()) {
      EditIterator ei = new EditIterator(iterator, Integer.MAX_VALUE);
      do {
        out.println(ei.getEntry());
      } while(ei.next());
    }
  }
}
