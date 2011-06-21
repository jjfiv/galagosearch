// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.featurefactory;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.core.retrieval.structured.StructuredIterator;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Parameters.Value;

/**
 * @author trevor
 * @author irmarc
 */
public class FeatureFactory {

  public FeatureFactory(Parameters parameters,
          String[][] sOperatorLookup, String[][] sFeatureLookup,
          String[] sTraversalList) {
    operatorLookup = new HashMap<String, OperatorSpec>();
    featureLookup = new HashMap<String, OperatorSpec>();
    this.parameters = parameters;

    for (String[] item : sFeatureLookup) {
      OperatorSpec operator = new OperatorSpec();
      operator.className = item[0];
      String operatorName = item[1];
      featureLookup.put(operatorName, operator);
    }

    for (String[] item : sOperatorLookup) {
      OperatorSpec operator = new OperatorSpec();
      operator.className = item[0];
      String operatorName = item[1];
      operatorLookup.put(operatorName, operator);
    }

    ArrayList<TraversalSpec> afterTraversals = new ArrayList<TraversalSpec>();
    ArrayList<TraversalSpec> beforeTraversals = new ArrayList<TraversalSpec>();
    ArrayList<TraversalSpec> insteadTraversals = new ArrayList<TraversalSpec>();

    for (Value value : parameters.list("traversals/traversal")) {
      String className = value.get("class");
      String order = value.get("order", "after");
      List<Value> params = value.list("parameters");
      if (className == null) {
        throw new RuntimeException("class is required in traversal declarations.");
      }

      TraversalSpec spec = new TraversalSpec();
      spec.className = className;
      if (params != null && params.size() > 0) {
        spec.parameters.copy(new Parameters(params.get(0)));
      }

      if (order.equals("before")) {
        beforeTraversals.add(spec);
      } else if (order.equals("after")) {
        afterTraversals.add(spec);
      } else if (order.equals("instead")) {
        insteadTraversals.add(spec);
      } else {
        throw new RuntimeException("order must be one of {before,after,instead}");
      }
    }

    // If the user doesn't want to replace the current pipeline, add in that pipeline
    if (insteadTraversals.size() == 0) {
      for (String className : sTraversalList) {
        TraversalSpec spec = new TraversalSpec();
        spec.className = className;
        spec.parameters = new Parameters();
        insteadTraversals.add(spec);
      }
    }

    traversals = new ArrayList<TraversalSpec>();
    traversals.addAll(beforeTraversals);
    traversals.addAll(insteadTraversals);
    traversals.addAll(afterTraversals);

    // Load external operators
    for (Value value : parameters.list("operators/operator")) {
      String className = value.get("class");
      String operatorName = value.get("name");
      List<Value> params = value.list("parameters");
      OperatorSpec spec = new OperatorSpec();

      if (params != null && params.size() > 0) {
        spec.parameters.copy(new Parameters(params.get(0)));
      }

      spec.className = className;
      operatorLookup.put(operatorName, spec);
    }

    // Load external features
    for (Value value : parameters.list("features/feature")) {
      String className = value.get("class");
      String operatorName = value.get("name");
      List<Value> params = value.list("parameters");
      OperatorSpec spec = new OperatorSpec();

      if (params != null && params.size() > 0) {
        spec.parameters.copy(new Parameters(params.get(0)));
      }

      spec.className = className;
      featureLookup.put(operatorName, spec);
    }
  }

  public static class OperatorSpec {

    public String className;
    public Parameters parameters = new Parameters();
  }

  public static class TraversalSpec {

    public String className;
    public Parameters parameters = new Parameters();
  }
  protected HashMap<String, OperatorSpec> featureLookup;
  protected HashMap<String, OperatorSpec> operatorLookup;
  protected List<TraversalSpec> traversals;
  protected Parameters parameters;

  public String getClassName(Node node) throws Exception {
    String operator = node.getOperator();

    if (operator.equals("feature")) {
      return getFeatureClassName(node.getParameters());
    }
    OperatorSpec operatorType = operatorLookup.get(operator);

    if (operatorType == null) {
      throw new IllegalArgumentException(
              "Unknown operator name: #" + operator);
    }
    return operatorType.className;
  }

  public String getFeatureClassName(Parameters parameters) throws Exception {
    if (parameters.containsKey("class")) {
      return parameters.get("class");
    }

    String name = parameters.get("name", parameters.get("default", (String) null));

    if (name == null) {
      throw new Exception(
              "Didn't find 'class', 'name', or 'default' parameter in this feature description.");
    }

    OperatorSpec operatorType = featureLookup.get(name);

    if (operatorType == null) {
      throw new Exception("Couldn't find a class for the feature named " + name + ".");
    }

    return operatorType.className;
  }

  @SuppressWarnings("unchecked")
  public Class<StructuredIterator> getClass(Node node) throws Exception {
    String className = getClassName(node);
    Class c = Class.forName(className);

    if (StructuredIterator.class.isAssignableFrom(c)) {
      return (Class<StructuredIterator>) c;
    } else {
      throw new Exception("Found a class, but it's not a StructuredIterator: " + className);
    }
  }

  public NodeType getNodeType(Node node) throws Exception {
    return new NodeType(getClass(node));
  }

  boolean isUsableConstructor(
          Class[] types,
          ArrayList<StructuredIterator> childIterators) {
    // We require at least one parameter in a usable constructor.
    if (types.length == 0) {
      return false;
    }

    // The first parameter needs to be a parameters object.
    if (!Parameters.class.isAssignableFrom(types[0])) {
      return false;
    }

    // Only the last parameter can be an array
    for (int i = 0; i < types.length - 1; ++i) {
      if (types[i].isArray()) {
        return false;
      }
    }

    boolean lastIsArray = types[types.length - 1].isArray();

    // Does it have an appropriate number of arguments?
    int iteratorArgs = 1 + childIterators.size();
    // If the last argument isn't an array, the argument list length
    // needs to match exactly.
    if (iteratorArgs != types.length && !lastIsArray) {
      return false;
    }
    // If the last argument is an array, we need to have enough arguments to
    // satisfy everything but the array parameter (since it's okay to put
    // zero things in an array).
    if (lastIsArray && iteratorArgs < types.length - 1) {
      return false;
    }

    // We now know it has the right number of args, so we check
    // the iterator parameters to make sure they match.
    int iteratorIndex = 0;
    int typeIndex;

    for (typeIndex = 1; typeIndex < types.length; typeIndex++) {
      Class currentType = types[typeIndex];
      if (currentType.isArray()) {
        // we'll check all the rest of the child iterators here
        for (; iteratorIndex < childIterators.size(); ++iteratorIndex) {
          StructuredIterator iterator = childIterators.get(iteratorIndex);
          if (!currentType.isAssignableFrom(iterator.getClass())) {
            return false;
          }
        }
      } else {
        if (iteratorIndex >= childIterators.size()) {
          return false;
        }

        StructuredIterator iterator = childIterators.get(iteratorIndex);
        if (!currentType.isAssignableFrom(iterator.getClass())) {
          return false;
        }
        iteratorIndex++;
      }
    }

    return true;
  }

  Object[] argsForConstructor(Class[] types,
          Parameters parameters,
          ArrayList<StructuredIterator> childIterators) {
    assert types.length > 0;

    Object[] args = new Object[types.length];
    // The first argument is a parameters object.
    args[0] = parameters;

    // The remaining arguments come from childIterators.
    for (int i = 1; i < args.length; ++i) {
      args[i] = childIterators.get(i - 1);
    }

    // If the last argument is an array, we need to convert any remaining
    // childIterators to an array form.
    Class lastType = types[types.length - 1];
    if (lastType.isArray()) {
      List<StructuredIterator> remaining =
              childIterators.subList(types.length - 2, childIterators.size());
      Object typedArray = Array.newInstance(lastType.getComponentType(), 0);
      Object[] remainingArray = remaining.toArray((Object[]) typedArray);
      args[args.length - 1] = remainingArray;
    }

    return args;
  }

  /**
   * Given a query node, generates the corresponding iterator object that can be used
   * for structured retrieval.  This method just calls getClass() on the node,
   * then instantiates the resulting class.
   *
   * If the class returned by getClass() is a ScoringFunction, it must contain
   * a constructor that takes a single Parameters object.  If the class returned by
   * getFeatureClass() is some kind of StructuredIterator (either a ScoreIterator,
   * ExtentIterator or CountIterator), it must take a Parameters object and an
   * ArrayList of DocumentDataIterators as parameters.
   */
  public StructuredIterator getIterator(Node node, ArrayList<StructuredIterator> childIterators) throws Exception {
    return getIterator(node, childIterators, null);
  }

  /**
   * We need the retrieval object in order to send back new queries to do mappings.
   * This also includes some special code, triggered by an operator/feature 
   * requiring the statistic 'retrieval' which sends the retrieval object to the 
   * constructor.
   * 
   * The only other change is a generalization of the original. Instead of only 
   * passing in singleton parameters, this now checks if it is a list and sends 
   * it as such if so, if not it will revert to sending it as a singleton. Again, this 
   * maintains backwards compatability. 
   * 
   * @author amarack
   */
  public StructuredIterator getIterator(Node node, ArrayList<StructuredIterator> childIterators, Retrieval retrieval) throws Exception {
    NodeType type = getNodeType(node);

    Constructor constructor = type.getConstructor();
    Class[] types = type.getParameterTypes(1 + childIterators.size());

    if (!isUsableConstructor(types, childIterators)) {
      throw new Exception("Couldn't find a reasonable constructor for type:" + type.toString());
    }

    Parameters parametersCopy = new Parameters();
    parametersCopy.copy(node.getParameters());
    Object[] args = argsForConstructor(constructor.getParameterTypes(),
            parametersCopy,
            childIterators);
    RequiredStatistics required =
            type.getIteratorClass().getAnnotation(RequiredStatistics.class);
    if (required != null) {
      for (String statistic : required.statistics()) {
        // About to get funky.
        if (statistic.equals("retrieval")) {
          if (retrieval == null) {
            parametersCopy.add(statistic, parameters.get(statistic, null));
          } else {
            // Indicate that we are passing the constructor a valid retrieval
            parametersCopy.add(statistic, parameters.get(statistic, "true"));

            // Reformat the arguments to make this happen
            Object[] new_args = new Object[args.length + 1];
            for (int i = 0; i < args.length; i++) {
              new_args[i] = args[i];
            }
            new_args[new_args.length - 1] = retrieval;
            args = new_args;
            // Find the correct constructor (slightly modified version of what 
            // is done above normally)
            for (Constructor cons : getClass(node).getConstructors()) {
              Class[] ctypes = cons.getParameterTypes();

              // The constructor needs at least one parameter.
              if (ctypes.length != args.length) {
                continue;
              }
              // The first class needs to be a Parameters object.
              if (!Parameters.class.isAssignableFrom(ctypes[0])) {
                continue;
              }
              // Check arguments for valid argument types.
              boolean validTypes = true;
              for (int i = 1; i < ctypes.length; ++i) {
                if (!type.isStructuredIteratorOrArray(ctypes[i]) && !Retrieval.class.isAssignableFrom(ctypes[i])
                        && !ctypes[i].isAssignableFrom(args[i].getClass())) {

                  validTypes = false;
                  break;
                }
              }
              // If everything looks good, return this constructor.
              if (validTypes) {
                constructor = cons;
                break;
              } else {
                System.err.println("Error on convert operator arguments!!");
              }
            }
          }

        } else {
          // Allow both singleton and list parameters to get sent to features
          if (parameters.containsKey(statistic)) {
            parametersCopy.add(statistic, parameters.list(statistic));
          } else {
            parametersCopy.add(statistic, parameters.get(statistic, null));
          }
        }
      }
    }
    return (StructuredIterator) constructor.newInstance(args);
  }

  public List<String> getTraversalNames() {
    ArrayList<String> result = new ArrayList<String>();
    for (TraversalSpec spec : traversals) {
      result.add(spec.className);
    }
    return result;
  }

  // TODO: change traversals to use a Retrieval object not a StructuredRetrieval
  public List<Traversal> getTraversals(Retrieval retrieval)
          throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
          IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    ArrayList<Traversal> result = new ArrayList<Traversal>();
    for (TraversalSpec spec : traversals) {
      Class<? extends Traversal> traversalClass =
              (Class<? extends Traversal>) Class.forName(spec.className);
      Constructor<? extends Traversal> constructor =
              traversalClass.getConstructor(Parameters.class, Retrieval.class);

      Parameters parametersCopy = spec.parameters.clone();
      RequiredStatistics required =
              traversalClass.getAnnotation(RequiredStatistics.class);
      if (required != null) {
        for (String statistic : required.statistics()) {
          parametersCopy.add(statistic, parameters.get(statistic, null));
        }
      }

      Traversal traversal = constructor.newInstance(parametersCopy, retrieval);
      result.add(traversal);
    }

    return result;
  }
}
