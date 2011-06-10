/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.parse;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.ExNihiloSource;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@OutputClass(className = "java.lang.String")
public class FileLineParser implements ExNihiloSource<String> {

  public Processor<String> processor;
  Parameters p;
  Counter lines;

  public FileLineParser(TupleFlowParameters parameters) {
    p = parameters.getXML();
    lines = parameters.getCounter("File Lines Read");
  }

  public void run() throws IOException {
    BufferedReader reader;
    for (String f : p.stringList("input")) {
      DocumentSplit split = new DocumentSplit();
      split.fileName = f;
      split.isCompressed = ( f.endsWith(".gz") || f.endsWith(".bz") );
      reader = UniversalParser.getBufferedReader( split );
      String line;
      while (null != (line = reader.readLine())) {
        if(lines != null) lines.increment();

        if (line.startsWith("#")) {
          continue;
        }
        processor.process(line);
      }
      reader.close();
    }
    processor.close();
  }

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }
}
