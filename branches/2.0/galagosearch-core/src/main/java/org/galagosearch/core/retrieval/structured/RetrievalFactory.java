/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.galagosearch.core.retrieval.MultiRetrieval;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Parameters.Value;

/**
 * Class for creating retrieval objects based on provided parameters
 *
 * @author irmarc
 */
public class RetrievalFactory {

  // Can't make one
  private RetrievalFactory() {}

  /* get retrieval object
   * cases:
   *  1 index path - local
   *  1 index path - proxy
   *  many index paths - multi - local
   *  many index paths - multi - drmaa
   */
  static public Retrieval instance(String path, Parameters parameters) throws IOException {
    if (path.startsWith("http://")) {
      // create a proxy, using the StructuredRetrievalProxy as the InvocationHandler
      InvocationHandler ih = new StructuredRetrievalProxy(path, parameters);
      return (Retrieval) Proxy.newProxyInstance(Retrieval.class.getClassLoader(),
                            new Class[] { Retrieval.class }, ih);
    } else {
      // check for drmaa
      return new StructuredRetrieval(path, parameters);
    }
  }

  static public Retrieval instance(Parameters parameters) throws IOException, Exception {
    List<Value> indexes = parameters.list("index");

    String path, id;

    // first check if there is only one index provided.
    if (indexes.size() == 1) {
      Value value = indexes.get(0);
      if (value.containsKey("path")) {
        path = value.get("path").toString();
      } else {
        path = value.toString();
      }
      return instance(path, parameters);
    }

    // otherwise we have a multi-index
    HashMap<String, ArrayList<Retrieval>> retrievals = new HashMap();
    for (Value value : indexes) {
      id = "all";
      if (value.containsKey("path")) {
        path = value.get("path").toString();
        if (value.containsKey("id")) {
          id = value.get("id").toString();
        }
      } else {
        path = value.toString();
      }
      if (!retrievals.containsKey(id)) {
        retrievals.put(id, new ArrayList<Retrieval>());
      }

      try {
        Retrieval r = instance(path, parameters);
        retrievals.get(id).add(r);
        if (!id.equals("all")) {
          retrievals.get("all").add(r); // Always put it in default as well
        }
      } catch (Exception e) {
        System.err.println("Unable to load index (" + id + ") at path " + path + ": " + e.getMessage());
      }
    }

    return new MultiRetrieval(retrievals, parameters);
  }
}
