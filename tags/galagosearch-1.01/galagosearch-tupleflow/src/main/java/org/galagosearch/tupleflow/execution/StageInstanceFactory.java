// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.tupleflow.execution;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.ExNihiloSource;
import org.galagosearch.tupleflow.FileOrderedReader;
import org.galagosearch.tupleflow.FileOrderedWriter;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.Order;
import org.galagosearch.tupleflow.OrderedCombiner;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.ReaderSource;
import org.galagosearch.tupleflow.Source;
import org.galagosearch.tupleflow.Splitter;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.TypeReader;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.StageInstanceDescription.PipeInput;
import org.galagosearch.tupleflow.execution.StageInstanceDescription.PipeOutput;

/**
 *
 * @author trevor
 */
public class StageInstanceFactory {
    NetworkedCounterManager counterManager;

    public StageInstanceFactory(NetworkedCounterManager counterManager) {
        this.counterManager = counterManager;
    }

    public class StepParameters implements TupleFlowParameters {
        Parameters xml;
        StageInstanceDescription instance;

        public StepParameters(Step o, StageInstanceDescription instance) {
            this.xml = o.getParameters();
            this.instance = instance;
        }

        public Counter getCounter(String name) {
            return counterManager.newCounter(
                    name, instance.getName(),
                    new Integer(instance.getIndex()).toString(), instance.getMasterURL());
        }

        public TypeReader getTypeReader(String specification) throws IOException {
            PipeOutput pipeOutput = instance.getReaders().get(specification);
            return StageInstanceFactory.getTypeReader(pipeOutput);
        }

        public Processor getTypeWriter(String specification) throws IOException {
            PipeInput pipeInput = instance.getWriters().get(specification);
            return StageInstanceFactory.getTypeWriter(pipeInput);
        }

        public boolean readerExists(String specification, String className, String[] order) {
            return instance.readerExists(specification, className, order);
        }

        public boolean writerExists(String specification, String className, String[] order) {
            return instance.writerExists(specification, className, order);
        }

        public Parameters getXML() {
            return xml;
        }
    }

    public ExNihiloSource instantiate(StageInstanceDescription instance)
            throws IncompatibleProcessorException, IOException {
        return (ExNihiloSource) instantiate(instance, instance.getStage().getSteps());
    }

    public org.galagosearch.tupleflow.Step instantiate(
            StageInstanceDescription instance,
            ArrayList<Step> steps)
            throws IncompatibleProcessorException, IOException {
        org.galagosearch.tupleflow.Step previous = null;
        org.galagosearch.tupleflow.Step first = null;

        for (Step step : steps) {
            org.galagosearch.tupleflow.Step current;

            if (step instanceof MultiStep) {
                current = instantiateMulti(instance, step);
            } else if (step instanceof InputStep) {
                current = instantiateInput(instance, (InputStep) step);
            } else if (step instanceof OutputStep) {
                current = instantiateOutput(instance, (OutputStep) step);
            } else {
                current = instantiateStep(instance, step);
            }

            if (first == null) {
                first = current;
            }
            if (previous != null) {
                ((Source) previous).setProcessor(current);
            }

            previous = current;
        }

        return first;
    }

    public org.galagosearch.tupleflow.Step instantiateStep(
            StageInstanceDescription instance,
            final Step step) throws IOException {
        org.galagosearch.tupleflow.Step object;

        try {
            Class objectClass = Class.forName(step.getClassName());
            Constructor parameterArgumentConstructor = null;
            Constructor noArgumentConstructor = null;

            for (Constructor c : objectClass.getConstructors()) {
                java.lang.reflect.Type[] parameters = c.getGenericParameterTypes();

                if (parameters.length == 0) {
                    noArgumentConstructor = c;
                } else if (parameters.length == 1 && parameters[0] == TupleFlowParameters.class) {
                    parameterArgumentConstructor = c;
                }
            }

            if (parameterArgumentConstructor != null) {
                object = (org.galagosearch.tupleflow.Step) parameterArgumentConstructor.newInstance(
                        new StepParameters(step, instance));
            } else if (noArgumentConstructor != null) {
                object = (org.galagosearch.tupleflow.Step) noArgumentConstructor.newInstance();
            } else {
                throw new IncompatibleProcessorException(
                        "Couldn't instantiate this class because " +
                        "no compatible constructor was found: " + step.getClassName());
            }
        } catch (Exception e) {
            throw (IOException) new IOException(
                    "Couldn't instantiate a step object: " + step.getClassName()).initCause(e);
        }

        return object;
    }

    public org.galagosearch.tupleflow.Step instantiateInput(
            StageInstanceDescription instance,
            InputStep step) throws IOException {
        PipeOutput pipeOutput = instance.getReaders().get(step.getId());
        return getTypeReaderSource(pipeOutput);
    }

    public org.galagosearch.tupleflow.Step instantiateOutput(
            StageInstanceDescription instance,
            final OutputStep step) throws IOException {
        PipeInput pipeInput = instance.getWriters().get(step.getId());
        return getTypeWriter(pipeInput);
    }

    private org.galagosearch.tupleflow.Step instantiateMulti(
            StageInstanceDescription instance,
            final Step step) throws IncompatibleProcessorException, IOException {
        MultiStep multiStep = (MultiStep) step;
        Processor[] processors = new Processor[multiStep.groups.size()];

        for (int i = 0; i < multiStep.groups.size(); i++) {
            ArrayList<Step> group = multiStep.groups.get(i);
            processors[i] = (org.galagosearch.tupleflow.Processor) instantiate(instance, group);
        }

        return new org.galagosearch.tupleflow.Multi(processors);
    }

    protected static Order createOrder(final DataPipe pipe) throws IOException {
        return createOrder(pipe.className, pipe.order);
    }

    public static Order createOrder(String className, String[] orderSpec) throws IOException {
        Order order;

        try {
            Class typeClass = Class.forName(className);
            org.galagosearch.tupleflow.Type type = (org.galagosearch.tupleflow.Type) typeClass.
                    getConstructor().newInstance();
            order = type.getOrder(orderSpec);
        } catch (Exception e) {
            throw (IOException) new IOException(
                    "Couldn't create an order object for type: " + className).initCause(e);
        }

        return order;
    }

    public ReaderSource getTypeReaderSource(PipeOutput pipeOutput) throws IOException {
        ReaderSource reader;

        if (pipeOutput == null) {
            return null;
        }

        Order order = createOrder(pipeOutput.getPipe());
        String[] fileNames = pipeOutput.getFileNames();

        if (fileNames.length > 1) {
            reader = OrderedCombiner.combineFromFiles(Arrays.asList(fileNames), order);
        } else {
            reader = new FileOrderedReader(fileNames[0], order);
        }
        return reader;
    }

    @SuppressWarnings(value = "unchecked")
    public static <T> ReaderSource<T> getTypeReader(final PipeOutput pipeOutput) throws IOException {
        ReaderSource<T> reader;

        if (pipeOutput == null) {
            return null;
        }

        Order order = createOrder(pipeOutput.getPipe());
        String[] fileNames = pipeOutput.getFileNames();

        if (fileNames.length > 100) {
            List<String> names = Arrays.asList(fileNames);
            ArrayList<String> reduced = new ArrayList<String>();

            // combine 20 files at a time
            for (int i = 0; i < names.size(); i += 20) {
                int start = i;
                int end = Math.min(names.size(), i + 20);
                List<String> toCombine = names.subList(start, end);

                reader = OrderedCombiner.combineFromFiles(toCombine, order);
                File temporary = Utility.createTemporary();
                FileOrderedWriter<T> writer = new FileOrderedWriter<T>(temporary, order);

                try {
                    reader.setProcessor(writer);
                } catch (IncompatibleProcessorException e) {
                    throw (IOException) new IOException("Incompatible processor for reader tuples").
                            initCause(e);
                }

                reader.run();

                reduced.add(temporary.toString());
                temporary.deleteOnExit();
            }

            reader = OrderedCombiner.combineFromFiles(reduced, order);
        } else if (fileNames.length > 1) {
            reader = OrderedCombiner.combineFromFiles(Arrays.asList(fileNames), order);
        } else {
            reader = new FileOrderedReader(fileNames[0], order);
        }
        return reader;
    }

    public static Processor getTypeWriter(final PipeInput pipeInput) throws IOException, IOException {
        Processor writer;

        if (pipeInput == null) {
            return null;
        }
        String[] fileNames = pipeInput.getFileNames();
        Order order = createOrder(pipeInput.getPipe());
        Order hashOrder = createOrder(pipeInput.getPipe().getClassName(), pipeInput.getPipe().getHash());

        assert order != null : "Order not found: " + Arrays.toString(pipeInput.getPipe().getOrder());

        try {
            if (fileNames.length == 1) {
                writer = new FileOrderedWriter(fileNames[0], order);
            } else {
                assert hashOrder != null : "Hash order not found: " + pipeInput.getPipe().getPipeName() + " " + pipeInput.getPipe().getHash();
                writer = Splitter.splitToFiles(fileNames, order, hashOrder);
            }
        } catch (IncompatibleProcessorException e) {
            throw (IOException) new IOException("Failed to create a typeWriter").initCause(e);
        }

        return writer;
    }
}
