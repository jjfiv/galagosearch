// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow.execution;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.galagosearch.tupleflow.Parameters.Parser;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author trevor
 */
public class JobConstructor extends DefaultHandler implements ErrorHandler {
    Pattern propertyPattern = Pattern.compile("\\$\\{([^}]+)\\}");
    HashMap<String, String> properties = new HashMap<String, String>();
    ArrayList<String> errors = new ArrayList<String>();
    Locator locator;
    JobHandler jobHandler;
    ErrorStore store;
    String fileName;

    /**
     * Creates a new instance of JobConstructor
     */
    public JobConstructor(String fileName, ErrorStore store) {
        this.fileName = fileName;
        this.jobHandler = new JobHandler();
        this.store = store;
    }

    public JobConstructor(ErrorStore store) {
        this.fileName = "none";
        this.jobHandler = new JobHandler();
        this.store = store;
    }

    public Job getJob() {
        return jobHandler.getJob();
    }

    public ErrorStore getErrorStore() {
        return store;
    }

    @Override
    public void setDocumentLocator(Locator locator) {
        this.locator = locator;
    }

    public void addError(String filename, SAXParseException exception) {
        store.addError(new FileLocation(filename, exception.getLineNumber(), exception.
                                        getColumnNumber()), exception.getMessage());
    }

    public void addError(String errorString) {
        store.addError(location(), errorString);
    }

    public void addWarning(String warning) {
        store.addWarning(location(), warning);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        jobHandler.endElement(uri, localName, qName);
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        jobHandler.startElement(uri, localName, qName, attributes);
    }

    private void verifyData(final Attributes attributes) {
        String className = attributes.getValue("class");
        String order = attributes.getValue("order");
        String type = attributes.getValue("type");
        String hash = attributes.getValue("hash");

        if (className == null) {
            addError("The data tag requires a class argument.");
            return;
        }

        if (type != null && !(type.equals("many") || type.equals("split"))) {
            addError("The type parameter, if specified, must be 'many' or 'split'.");
            return;
        }

        if (!Verification.isClassAvailable(className)) {
            addError("Couldn't find class: " + className);
            return;
        }

        if (order != null && !Verification.isOrderAvailable(className, order.split(" "))) {
            addError("Couldn't find order: " + order);
        }

        if (hash != null && !Verification.isOrderAvailable(className, hash.split(" "))) {
            addError("Couldn't find order: " + hash);
        }
    }

    private void verifyStep(final Attributes attributes, final TupleFlowParameters parameters) {
        String className = attributes.getValue("class");

        if (className == null) {
            addError("The step tag requires a class argument.");
            return;
        }

        if (!Verification.isClassAvailable(className)) {
            addError("Couldn't find a class named " + className);
        }

        try {
            Class c = Class.forName(className);
            Class[] parameterTypes = new Class[]{TupleFlowParameters.class, ErrorHandler.class};
            Method m = c.getDeclaredMethod("verify", parameterTypes);
            m.invoke(null, parameters, this);
        } catch (Exception e) {
            addError("Exception thrown during step verification: " + className + " " + e.getCause().
                     getMessage());
        }
    }

    @Override
    public void characters(char[] buffer, int offset, int length) throws SAXException {
        jobHandler.characters(buffer, offset, length);
    }

    public FileLocation location() {
        return new FileLocation(fileName, locator);
    }
    // Handlers section
    public class StageHandler extends StackHandler {
        Stage stage = new Stage();

        @Override
        public void startHandler(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            if (attributes.getValue("id") == null) {
                addError(
                        "'id' is a required attribute of 'stage'.");
            }
            stage.name = attributes.getValue("id");
            stage.location = location();
        }

        @Override
        public void endChild(StackHandler handler, String uri, String localName, String qName) throws SAXException {
            if (handler instanceof StageConnectionsHandler) {
                stage.connections = ((StageConnectionsHandler) handler).getConnectionPoints();
            } else if (handler instanceof StepsHandler) {
                stage.steps = ((StepsHandler) handler).getSteps();
            }
        }

        @Override
        public void unhandledStartElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            if (qName.equals("steps")) {
                addHandler(new StepsHandler(), uri, localName, qName, attributes);
            } else if (qName.equals("connections")) {
                addHandler(new StageConnectionsHandler(), uri, localName, qName, attributes);
            } else {
                addError("Unrecognized tag: '" + qName + "', expecting 'steps' or 'connections'.");
                addHandler(new StackHandler()); // ignore subtags
            }
        }

        private Stage getStage() {
            return stage;
        }
    }

    public class StageConnectionsHandler extends StackHandler {
        HashMap<String, StageConnectionPoint> connectionPoints = new HashMap<String, StageConnectionPoint>();

        public HashMap<String, StageConnectionPoint> getConnectionPoints() {
            return connectionPoints;
        }

        @Override
        public void unhandledStartElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            String id = attributes.getValue("id");
            String clazz = attributes.getValue("class");
            String orderSpec = attributes.getValue("order");
            String[] order = new String[0];

            if (!(qName.equals("input") || qName.equals("output"))) {
                addError("Expected 'input' or 'output', not " + qName);
                return;
            }

            if (id == null) {
                addError("'id' is a required attribute of '" + qName + "'.");
                return;
            }

            if (clazz == null) {
                addError("'class' is a required attribute of '" + qName + "'.");
                return;
            }

            if (orderSpec != null) {
                order = orderSpec.split(" ");
            }
            if (qName.equals("input")) {
                connectionPoints.put(id,
                                     new StageConnectionPoint(ConnectionPointType.Input,
                                                              id, clazz, order,
                                                              location()));
            } else if (qName.equals("output")) {
                connectionPoints.put(id,
                                     new StageConnectionPoint(ConnectionPointType.Output,
                                                              id, clazz, order,
                                                              location()));
            } else {
                addError("Tag '" + qName + "' isn't legal in the connections section of a stage.");
            }
        }
    }

    public class JobHandler extends StackHandler {
        Job job = new Job();

        @Override
        public void endChild(StackHandler handler, String uri, String localName, String qName) throws SAXException {
            if (handler instanceof ConnectionsHandler) {
                job.connections = ((ConnectionsHandler) handler).getConnections();
            } else if (handler instanceof StagesHandler) {
                job.stages = ((StagesHandler) handler).getStages();
            }
        }

        @Override
        public void unhandledStartElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            if (qName.equals("job")) {
                // do nothing
            } else if (qName.equals("connections")) {
                addHandler(new ConnectionsHandler(), uri, localName, qName, attributes);
            } else if (qName.equals("stages")) {
                addHandler(new StagesHandler(), uri, localName, qName, attributes);
            } else if (qName.equals("property")) {
                String name = attributes.getValue("name");
                String value = attributes.getValue("value");

                if (name == null || value == null) {
                    addError("The 'property' tag requries 'name' and 'value' attributes.");
                } else {
                    job.properties.put(name, value);
                    properties.put(name, value);
                }
            } else {
                addError("Unrecognized tag: '" + qName + "', expecting 'connections' or 'stages'.");
                addHandler(new StackHandler()); // ignore subtags
            }
        }

        public Job getJob() {
            return job;
        }
    }

    public class StagesHandler extends StackHandler {
        TreeMap<String, Stage> stages = new TreeMap<String, Stage>();

        @Override
        public void endChild(StackHandler handler, String uri, String localName, String qName) throws SAXException {
            if (handler instanceof StageHandler) {
                Stage s = ((StageHandler) handler).getStage();
                if (s.name != null) {
                    stages.put(s.name, s);
                }
            }
        }

        @Override
        public void unhandledStartElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            if (qName.equals("stage")) {
                addHandler(new StageHandler(), uri, localName, qName, attributes);
            } else {
                addError("Unrecognized tag: '" + qName + "', expecting 'stage'.");
                addHandler(new StackHandler()); // ignore subtags
            }
        }

        private TreeMap<String, Stage> getStages() {
            return stages;
        }
    }

    public class StepsHandler extends StackHandler {
        ArrayList<Step> steps = new ArrayList<Step>();

        public ArrayList<Step> getSteps() {
            return steps;
        }

        @Override
        public void endChild(StackHandler handler, String uri, String localName, String qName) throws SAXException {
            if (handler instanceof StepHandler) {
                steps.add(((StepHandler) handler).getStep());
            } else if (handler instanceof MultiHandler) {
                steps.add(((MultiHandler) handler).getStep());
            } else {
                addError("Unknown handler type: " + handler.getClass().toString());
            }
        }

        @Override
        public void unhandledStartElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            if (qName.equals("step")) {
                addHandler(new StepHandler(), uri, localName, qName, attributes);
            } else if (qName.equals("multi")) {
                addHandler(new MultiHandler(), uri, localName, qName, attributes);
            } else if (qName.equals("output")) {
                if (attributes.getValue("id") == null) {
                    addError(
                            "'output' requires an 'id' attribute.");
                }
                steps.add(new OutputStep(location(), attributes.getValue("id")));
            } else if (qName.equals("input")) {
                if (attributes.getValue("id") == null) {
                    addError(
                            "'input' requires an 'id' attribute.");
                }
                steps.add(new InputStep(location(), attributes.getValue("id")));
            } else {
                addError(
                        "Found '" + qName + "', but was expecting 'step', 'multi', 'input' or 'output'.");
            }
        }
    }

    public class StepHandler extends StackHandler {
        Step step;
        Parameters parameters = new Parameters();
        Parser parametersHandler;

        public StepHandler() throws SAXException {
            parametersHandler = parameters.getParseHandler();
            parametersHandler.startElement("", "", "parameters", null);
        }

        @Override
        public void startHandler(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            String className = attributes.getValue("class");

            if (className == null) {
                addError("'class' is a required attribute of 'step'.");
            }

            step = new Step(location(), className, parameters);
        }

        @Override
        public void unhandledCharacters(char[] buffer, int offset, int length) throws SAXException {
            String item = new String(buffer, offset, length);
            Matcher m = propertyPattern.matcher(item);
            StringBuffer stringBuffer = new StringBuffer();

            while (m.find()) {
                String key = m.group(1);
                String value = properties.get(key);

                if (value == null) {
                    addError("Couldn't find a property named '" + key + "'");
                    m.appendReplacement(stringBuffer, "");
                } else {
                    m.appendReplacement(stringBuffer, value);
                }
            }
            m.appendTail(stringBuffer);

            buffer = stringBuffer.toString().toCharArray();
            parametersHandler.characters(buffer, 0, buffer.length);
        }

        @Override
        public void unhandledStartElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            parametersHandler.startElement(uri, localName, qName, attributes);
        }

        @Override
        public void unhandledEndElement(String uri, String localName, String qName) throws SAXException {
            parametersHandler.endElement(uri, localName, qName);
        }

        public Step getStep() {
            return step;
        }
    }

    public class MultiHandler extends StackHandler {
        MultiStep multi = new MultiStep();

        public MultiStep getStep() {
            return multi;
        }

        @Override
        public void endChild(StackHandler handler, String uri, String localName, String qName) throws SAXException {
            ArrayList<Step> steps = ((StepsHandler) handler).getSteps();
            multi.groups.add(steps);
        }

        @Override
        public void unhandledStartElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            if (!qName.equals("group")) {
                addError("Found '" + qName + "' but was expecting 'group'.");
                return;
            }

            addHandler(new StepsHandler());
        }
    }

    public class ConnectionsHandler extends StackHandler {
        public ArrayList<Connection> connections = new ArrayList<Connection>();

        public ArrayList<Connection> getConnections() {
            return connections;
        }

        @Override
        public void unhandledStartElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            if (!qName.equals("connection")) {
                addError("Found '" + qName + "' but expected 'connection'");
            }

            addHandler(new ConnectionHandler(), uri, localName, qName, attributes);
        }

        @Override
        public void endChild(StackHandler handler, String uri, String localName, String qName) throws SAXException {
            connections.add(((ConnectionHandler) handler).getConnection());
        }
    }

    public class ConnectionHandler extends StackHandler {
        public Connection connection;

        public Connection getConnection() {
            return connection;
        }

        @Override
        public void startHandler(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            String className = attributes.getValue("class");

            if (className == null) {
                addError("'class' is a required attribute of a connection.");
            }

            String orderSpec = attributes.getValue("order");
            String[] order = new String[0];

            if (orderSpec == null) {
                addError("'order' is a required attribute of a connection.");
            } else {
                order = orderSpec.split(" ");
            }

            String hashSpec = attributes.getValue("hash");
            String[] hash = null;

            if (hashSpec != null) {
                hash = hashSpec.split(" ");
            }
            String hashCount = attributes.getValue("hashCount");
            int count = -1;

            if (hashCount != null) {
                try {
                    count = Integer.parseInt(hashCount);
                } catch (NumberFormatException e) {
                    addError(
                            "Expected a numeric argument for 'count', but saw '" + hashCount + "' instead.");
                }
            }

            Verification.requireClass(className, JobConstructor.this);
            connection = new Connection(location(), className, order, hash, count);
        }

        @Override
        public void unhandledStartElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            String stageName = attributes.getValue("stage");
            String pointName = attributes.getValue("endpoint");

            if (!qName.equals("input") && !qName.equals("output")) {
                addError("Found '" + qName + "' but expected 'input' or 'output'.");
                return;
            }

            if (stageName == null) {
                addError("'stage' is a required attribute of '" + qName + "'.");
                return;
            }

            if (pointName == null) {
                addError("'endpoint' is a required attribute of '" + qName + "'.");
                return;
            }

            if (qName.equals("input")) {
                connection.inputs.add(new ConnectionEndPoint(location(), stageName, pointName,
                                                             ConnectionPointType.Input));
            } else if (qName.equals("output")) {
                String assignmentString = attributes.getValue("assignment");

                if (assignmentString == null) {
                    addError("'assignment' is a required attribute of 'output'.");
                    return;
                }

                ConnectionAssignmentType assignment;

                if (assignmentString.equals("one")) {
                    assignment = ConnectionAssignmentType.One;
                } else if (assignmentString.equals("each")) {
                    assignment = ConnectionAssignmentType.Each;
                } else if (assignmentString.equals("combined")) {
                    assignment = ConnectionAssignmentType.Combined;
                } else {
                    addError("'assignment' needs to be either 'one', 'each', or 'combined' (not '" +
                             assignmentString + "').");
                    return;
                }

                ConnectionEndPoint point = new ConnectionEndPoint(location(),
                                                                  stageName,
                                                                  pointName,
                                                                  assignment,
                                                                  ConnectionPointType.Output);

                connection.outputs.add(point);
            }
        }
    }
}
