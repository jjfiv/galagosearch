// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 *
 * @author trevor
 */
public class SequentialCombiner<T> implements ExNihiloSource<T>, TypeReader<T> {
    ArrayList<String> filenames;
    Order<T> order;
    FileOrderedReader<T> active;
    public Processor<T> processor;
    boolean closeOnExit = true;
    Logger logger;

    /** Creates a new instance of SequentialCombiner */
    public SequentialCombiner(List<String> filenames, Order<T> order) {
        this.filenames = new ArrayList<String>(filenames);
        this.order = order;
        this.logger = Logger.getLogger(SequentialCombiner.class.toString());
    }

    public Class<T> getOutputClass() {
        return order.getOrderedClass();
    }

    public void setProcessor(Step processor) throws IncompatibleProcessorException {
        Linkage.link(this, processor);
    }

    public void run() throws IOException {
        logger.info("Starting");

        for (String filename : filenames) {
            logger.info("Opening: " + filename);
            FileOrderedReader<T> reader = new FileOrderedReader<T>(filename, order);
            T object;

            while ((object = reader.read()) != null) {
                processor.process(object);
            }

            reader.close();
            logger.info("Closing: " + filename);
        }

        logger.info("Finished");
        if (closeOnExit) {
            processor.close();
        }
    }

    public static <S> SequentialCombiner<S> combineFromFiles(
            List<String> filenames,
            Order<S> order) throws IOException {
        return new SequentialCombiner<S>(filenames, order);
    }

    public T read() throws IOException {
        if (active == null) {
            if (filenames.size() == 0) {
                logger.info("Complete");
                return null;
            } else {
                logger.info("Opening: " + filenames.get(0));
                active = new FileOrderedReader<T>(filenames.get(0), order);
                filenames.remove(0);
            }
        }

        T object = active.read();

        while (object == null && filenames.size() > 0) {
            logger.info("Opening: " + filenames.get(0));
            active = new FileOrderedReader<T>(filenames.get(0), order);
            filenames.remove(0);
            object = active.read();
        }

        return object;
    }
}
