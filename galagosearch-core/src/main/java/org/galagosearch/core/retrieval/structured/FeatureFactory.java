// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.scoring.DirichletScorer;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class FeatureFactory {
    static String[][] sOperatorLookup = {
        {FilteredCombinationIterator.class.getName(), "filter"},
        {UnfilteredCombinationIterator.class.getName(), "combine"},
        {SynonymIterator.class.getName(), "syn"},
        {SynonymIterator.class.getName(), "synonym"},
        {ExtentInsideIterator.class.getName(), "inside"},
        {OrderedWindowIterator.class.getName(), "ordered"},
        {OrderedWindowIterator.class.getName(), "od"},
        {UnorderedWindowIterator.class.getName(), "unordered"},
        {UnorderedWindowIterator.class.getName(), "uw"},
        {ScaleIterator.class.getName(), "scale"}
    };
    static String[][] sFeatureLookup = {
        {DirichletScorer.class.getName(), "dirichlet"}
    };
    HashMap<String, String> featureLookup;
    HashMap<String, String> operatorLookup;
    Parameters parameters;

    public FeatureFactory(Parameters parameters) {
        operatorLookup = new HashMap<String, String>();
        featureLookup = new HashMap<String, String>();
        this.parameters = parameters;
        
        for (String[] item : sFeatureLookup) {
            featureLookup.put(item[1], item[0]);
        }

        for (String[] item : sOperatorLookup) {
            operatorLookup.put(item[1], item[0]);
        }
    }

    public String getClassName(Node node) throws Exception {
        String operator = node.getOperator();

        if (operator.equals("feature")) {
            return getFeatureClassName(node.getParameters());
        }
        String className = operatorLookup.get(operator);

        if (className == null) {
            throw new IllegalArgumentException(
                    "Unknown operator name: #" + operator);
        }
        return className;
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

        String className = featureLookup.get(name);

        if (className == null) {
            throw new Exception("Couldn't find a class for the feature named " + name + ".");
        }

        return className;
    }

    @SuppressWarnings("unchecked")
    public Class<StructuredIterator> getClass(Node node) throws Exception {
        String className = getClassName(node);
        Class c = Class.forName(className);

        if (StructuredIterator.class.isAssignableFrom(c)) {
            return (Class<StructuredIterator>) c;
        } else {
            throw new Exception("Found a class, but it's not a DocumentDataIterator: " + className);
        }
    }
    
    public NodeType getNodeType(Node node) throws Exception {
        return new NodeType(getClass(node));
    }

    boolean isUsableConstructor(
            Class[] types,
            ArrayList<StructuredIterator> childIterators) {
        // We require at least one parameter in a usable constructor.
        if (types.length == 0)
            return false;
            
        // The first parameter needs to be a parameters object.
        if (!Parameters.class.isAssignableFrom(types[0]))
            return false;
            
        // Only the last parameter can be an array
        for (int i = 0; i < types.length-1; ++i) {
            if (types[i].isArray())
                return false;
        }
        
        boolean lastIsArray = types[types.length-1].isArray();
        
        // Does it have an appropriate number of arguments?
        int iteratorArgs = 1 + childIterators.size();
        // If the last argument isn't an array, the argument list length
        // needs to match exactly.
        if (iteratorArgs != types.length && !lastIsArray)
            return false;
        // If the last argument is an array, we need to have enough arguments to
        // satisfy everything but the array parameter (since it's okay to put
        // zero things in an array).
        if (lastIsArray && iteratorArgs < types.length-1)
            return false;
        
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
                if (iteratorIndex >= childIterators.size())
                    return false;

                StructuredIterator iterator = childIterators.get(iteratorIndex);
                if (!currentType.isAssignableFrom(iterator.getClass()))
                    return false;
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
            args[i] = childIterators.get(i-1);
        }

        // If the last argument is an array, we need to convert any remaining
        // childIterators to an array form.
        Class lastType = types[types.length-1];
        if (lastType.isArray()) {
            List<StructuredIterator> remaining =
                childIterators.subList(types.length-2, childIterators.size());
            Object typedArray = Array.newInstance(lastType.getComponentType(), 0);
            Object[] remainingArray = remaining.toArray((Object[]) typedArray);
            args[args.length-1] = remainingArray;
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
        NodeType type = getNodeType(node);
        
        Constructor constructor = type.getConstructor();
        Class[] types = type.getParameterTypes(1 + childIterators.size());
            
        if (!isUsableConstructor(types, childIterators)) {
            throw new Exception("Couldn't find a reasonable constructor.");
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
                parametersCopy.add(statistic, parameters.get(statistic, null));
            }
        }
        return (StructuredIterator) constructor.newInstance(args);
    }
}
