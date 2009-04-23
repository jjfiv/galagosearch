// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.StageInstanceFactory;
import org.galagosearch.tupleflow.execution.Verification;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * Combines many streams of data together, sequentially.  This is a
 * replacement for a typical input step.
 * 
 * @author trevor
 */
@Verified
public class StreamCombiner<T> implements ExNihiloSource<T> {
    ArrayList<TypeReader<T>> readers;
    Comparator<T> comparator;
    public Processor<T> processor;

    class ReaderWrapper implements Comparable<ReaderWrapper> {
        public TypeReader<T> reader;
        public T top;
        public T last;

        public ReaderWrapper(TypeReader<T> r) {
            reader = r;
            top = null;
            last = null;
        }

        public int compareTo(ReaderWrapper other) {
            if (top == null && other.top == null) {
                return 0;
            }
            if (top == null) {
                return 1;
            }
            if (other.top == null) {
                return -1;
            }
            return comparator.compare(this.top, other.top);
        }

        public boolean read() throws IOException {
            last = top;
            top = reader.read();
            assert last == null || top == null || comparator.compare(last, top) <= 0 : last.toString() + " " + top.
                    toString() + " " + reader.toString() + " " + reader.hashCode();

            return top != null;
        }
    }

    @SuppressWarnings("unchecked")
    public StreamCombiner(TupleFlowParameters parameters) throws IOException {
        List<String> inputs = parameters.getXML().stringList("input");
        String className = parameters.getXML().get("class");
        String[] orderSpec = parameters.getXML().get("order", "").split(" ");
        Order<T> order = StageInstanceFactory.createOrder(className, orderSpec);
        comparator = order.lessThan();
        readers = new ArrayList<TypeReader<T>>();

        for (String input : inputs) {
            readers.add((TypeReader<T>) parameters.getTypeReader(input));
        }
    }

    public void run() throws IOException {
        PriorityQueue<ReaderWrapper> wrappers = new PriorityQueue<ReaderWrapper>();

        for (TypeReader<T> reader : readers) {
            ReaderWrapper rw = new ReaderWrapper(reader);
            if (rw.read()) {
                wrappers.add(rw);
            }
        }

        while (wrappers.size() > 0) {
            ReaderWrapper rw = wrappers.poll();
            T top = rw.top;

            processor.process(rw.top);
            if (rw.read()) {
                wrappers.add(rw);
            }
        }

        processor.close();
    }

    public void setProcessor(Step processor) throws IncompatibleProcessorException {
        Linkage.link(this, processor);
    }

    public static String getInputClass(TupleFlowParameters parameters) {
        return parameters.getXML().get("class");
    }

    public static String getOutputClass(TupleFlowParameters parameters) {
        return parameters.getXML().get("class");
    }

    public static String[] getOutputOrder(TupleFlowParameters parameters) {
        String[] orderSpec = parameters.getXML().get("order", "").split(" ");
        return orderSpec;
    }

    public static String[] getInputOrder(TupleFlowParameters parameters) {
        String[] orderSpec = parameters.getXML().get("order", "").split(" ");
        return orderSpec;
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) throws ClassNotFoundException {
        Verification.requireParameters(new String[]{"input", "class"}, parameters.getXML(), handler);

        List<String> inputs = parameters.getXML().stringList("input");
        String cls = parameters.getXML().get("class");

        for (String input : inputs) {
            Verification.verifyTypeReader(input, Class.forName(cls), parameters, handler);
        }
    }
}
