// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.mem;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.galagosearch.core.parse.NumberedDocument;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Sorter;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;


@Verified
@InputClass( className = "org.galagosearch.core.parse.NumberedDocument")
@OutputClass( className = "org.galagosearch.core.parse.NumberedDocument")
public class MemoryChecker extends StandardStep<NumberedDocument, NumberedDocument> {
  
  private Logger logger = Logger.getLogger(MemoryChecker.class.toString());
  MemoryPoolMXBean heap = null;
  long docCount = 0;
  
  public MemoryChecker(){
    heap = getMemoryBean();
    
  }
  
  public void process(NumberedDocument doc) throws IOException {
    if(docCount % 1000 == 0){
      System.err.print("count:" + docCount + " ");
      printMemoryUsage(heap);
    }
    docCount ++;
    
    processor.process(doc);
  }
  
  private static void printMemoryUsage(MemoryPoolMXBean heap){
    System.gc();

    MemoryUsage usage = heap.getUsage();
    System.err.println(heap.getName() + " -usage- " + usage.getUsed() + " -human- " + (usage.getUsed() / 1024.0 / 1024.0));
  }
  
  private static MemoryPoolMXBean getMemoryBean(){
    System.gc();

    List<MemoryPoolMXBean> pools = ManagementFactory.getMemoryPoolMXBeans();
    long curMax = 0;
    MemoryPoolMXBean heap = null;
    
    for (MemoryPoolMXBean pool : pools) {
      if (pool.getType() != MemoryType.HEAP) {
        continue;
      }
      MemoryUsage memusage = pool.getUsage();
      long max = memusage.getMax();
      if(max > curMax)
        heap = pool;
    }
    return heap;
  }
}
