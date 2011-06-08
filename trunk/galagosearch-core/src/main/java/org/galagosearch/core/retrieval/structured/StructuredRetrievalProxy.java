/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.galagosearch.core.retrieval.ScoredDocument;
import org.galagosearch.tupleflow.Parameters;

/**
 * This acts as the client-side stub for forwarding requests across 
 * a network connection to a remote index.
 *
 * @author irmarc
 */
public class StructuredRetrievalProxy implements InvocationHandler, Runnable {

  String indexUrl;
  HashSet<String> unImplemented;
  // For async execution
  Thread queryRunner = null;
  Object[] argHolder;

  public StructuredRetrievalProxy(String url, Parameters parameters) throws IOException {
    this.indexUrl = url + "/stream";
    unImplemented = new HashSet<String>();
  }

  public void close() throws IOException {
    // Nothing to do - index is serving remotely - possibly to several handlers
  }

  public Object invoke(Object caller, Method method, Object[] args) throws Throwable {
    return invoke(method.getName(), args);
  }

  public Object invoke(String methodName, Object[] args) throws Throwable {

    // Check to make sure we shouldn't skip it
    if (unImplemented.contains(methodName)) {
      throw new UnsupportedOperationException("Proxy class does not support this operation.");
    }

    // Not pretty, but these need to be treated special
    if (methodName.equals("runAsynchronousQuery")) {
      argHolder = args;
      queryRunner = new Thread(this);
      queryRunner.start();
      return null;
    } else if (methodName.equals("waitForAsynchronousQuery")) {
      if (queryRunner != null) {
        queryRunner.join();
        queryRunner = null;
      }
      return null;
    }

    // Otherwise do it normally
    URL resource = new URL(this.indexUrl);
    HttpURLConnection connection = (HttpURLConnection) resource.openConnection();
    connection.setRequestMethod("GET");
    connection.setDoOutput(true);
    connection.setDoInput(true);
    connection.connect();

    // Write data directly to the stream
    OutputStream writeStream = connection.getOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(writeStream);

    // First the Method, which is not serializable directly
    oos.writeUTF(methodName);

    // Write length of arguments
    oos.writeShort((short) args.length);

    // Types of arguments
    for (int i = 0; i < args.length; i++) {
      oos.writeObject(args[i].getClass());
    }

    // Now write them out via serialization
    for (int i = 0; i < args.length; i++) {
      Object arg = args[i];
      oos.writeObject(arg);
    }

    // Wait for response
    InputStream stream = connection.getInputStream();

    // Now get the response and re-instantiate
    // This requires that the return type is serializable
    ObjectInputStream ois = new ObjectInputStream(stream);
    Object response = ois.readObject();

    // Do we want to keep reconnecting and disconnecting?
    // Maybe a persistent connection is worth it?
    connection.disconnect();
    return response;
  }

  // We have to implement the asynchronous execution locally - we don't
  // remote async...
  public void run() {
    // Need to get the correct arguments to pass forward
    Object[] newArgs = new Object[2];
    newArgs[0] = argHolder[0];
    newArgs[1] = argHolder[1];

    // Typecast the results properly
    List<ScoredDocument> aggregatedResults = (List<ScoredDocument>) argHolder[2];
    List<String> errors = (List<String>) argHolder[3];
    ScoredDocument[] results = new ScoredDocument[0];
    try {
      results = (ScoredDocument[]) invoke("runRankedQuery", newArgs);
      synchronized (aggregatedResults) {
        aggregatedResults.addAll(Arrays.asList(results));
      }
    } catch (Throwable t) {
      System.err.println("StructuredRetrievalProxy  ERROR RETRIEVING: " + t.toString());
      t.printStackTrace(System.err);
      synchronized (errors) {
        errors.add(t.toString());
      }
    }
  }
}
