// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.tupleflow;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import org.galagosearch.tupleflow.execution.Step;

/**
 * Lots of static methods here that have broad use.
 *
 * @author trevor
 */
public class Utility {

  private static Logger LOG = Logger.getLogger(Utility.class.getName());

  /**
   * Put all initialization here
   */
  static {
    // Initialization of the tmp locations

    // try to find a prefs file for it
    HashSet<String> holder = new HashSet<String>();
    try {
      String homeDirectory = System.getProperty("user.home");
      File prefsFile = new File(homeDirectory + "/" + ".galagotmp");
      if (prefsFile.exists()) {
        BufferedReader reader = new BufferedReader(new FileReader(prefsFile));
        String line;

        while ((line = reader.readLine()) != null) {
          String trimmed = line.trim();
          if (!holder.contains(trimmed)) {
            holder.add(trimmed);
          }
        }
        reader.close();
      }
    } catch (IOException ioe) {
      LOG.warning("Unable to locate pref file. Using default temp location.");
    }
    roots = holder.toArray(new String[0]);
  }

  /**
   * <p>If the parent directories for this file don't exist, this function creates them.</p>
   *
   * <p>Often we want to create a file, but we don't yet know if the parent path has been
   * created yet.  Call this function immediately before opening a file for writing to
   * make sure those directories have been created.</p>
   *
   * @param filename A filename that will soon be opened for writing.
   */
  public static void makeParentDirectories(File f) {
    File parent = f.getParentFile();
    if (parent != null) {
      parent.mkdirs();
    }
  }


  public static void makeParentDirectories(String filename) {
    makeParentDirectories(new File(filename));
  }

  /**
   * Builds a simple Sorter step that can be added to a TupleFlow stage.
   *
   * @param sortOrder An order object representing how and what to sort.
   * @return a Step object that can be added to a TupleFlow Stage.
   */
  public static Step getSorter(Order sortOrder) {
    return getSorter(sortOrder, null);
  }

  /**
   * Builds a Sorter step with a reducer that can be added to a TupleFlow stage.
   *
   * @param sortOrder An order object representing how and what to sort.
   * @param reducerClass The class of a reducer object that can reduce this data.
   * @return a Step object that can be added to a TupleFlow Stage.
   */
  public static Step getSorter(Order sortOrder, Class reducerClass) {
    Parameters p = new Parameters();
    p.add("class", sortOrder.getOrderedClass().getName());
    p.add("order", Utility.join(sortOrder.getOrderSpec()));
    if (reducerClass != null) {
      try {
        reducerClass.asSubclass(Reducer.class);
      } catch (ClassCastException e) {
        throw new IllegalArgumentException("getSorter called with a reducerClass argument "
                + "which is not actually a reducer: "
                + reducerClass.getName());
      }
      p.add("reducer", reducerClass.getName());
    }
    return new Step(Sorter.class, p);
  }

  /**
   * Finds a free port to listen on.  Useful for starting up internal web servers.
   * (copied from chaoticjava.com)
   */
  public static int getFreePort() throws IOException {
    ServerSocket server = new ServerSocket(0);
    int port = server.getLocalPort();
    server.close();
    return port;
  }

  /**
   * Determines if the specified port is available for use.
   */
  public static boolean isFreePort(int portnum) {
    try {
      ServerSocket server = new ServerSocket(portnum);
      int boundPort = server.getLocalPort();
      server.close();
      return (boundPort == portnum);
    } catch (IOException ioe) {
      return false;
    }
  }

  public static boolean isInteger(String s) {
    try {
      Integer.parseInt(s);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public static String wrap(String t) {
    int start = 0;
    StringBuilder result = new StringBuilder();

    while (t.length() > start + 50) {
      int end = t.indexOf(" ", start + 50);

      if (end < 0) {
        break;
      }
      result.append(t, start, end);
      result.append('\n');
      start = end + 1;
    }

    result.append(t.substring(start));
    return result.toString();
  }

  public static String escape(String raw) {
    StringBuilder builder = new StringBuilder();

    for (int i = 0; i < raw.length(); i++) {
      char c = raw.charAt(i);

      if (c == '"') {
        builder.append("&quot;");
      } else if (c == '&') {
        builder.append("&amp;");
      } else if (c == '<') {
        builder.append("&gt;");
      } else if (c == '>') {
        builder.append("&lt;");
      } else if (c <= 127) {
        builder.append(c);
      } else {
        int unsigned = ((int) c) & 0xFFFF;

        builder.append("&#");
        builder.append(unsigned);
        builder.append(";");
      }
    }

    return builder.toString();
  }

  public static String strip(String source, String suffix) {
    if (source.endsWith(suffix)) {
      return source.substring(0, source.length() - suffix.length());
    }

    return null;
  }

  /**
   * <p>Splits args into an array of flags and an array of parameters.</p>
   *
   * <p>This method assumes that args is an array of strings, where some of those
   * strings are flags (they start with '-') and the others are non-flag arguments.
   * This splits those into two arrays so they can be processed separately.</p>
   *
   * @param args
   * @return An array of length 2, where the first element is an array of flags
   *         and the second is an array of arguments.
   */
  public static String[][] filterFlags(String[] args) {
    ArrayList<String> flags = new ArrayList<String>();
    ArrayList<String> nonFlags = new ArrayList<String>();

    for (String arg : args) {
      if (arg.startsWith("-")) {
        flags.add(arg);
      } else {
        nonFlags.add(arg);
      }
    }

    String[][] twoArrays = new String[2][];
    twoArrays[0] = flags.toArray(new String[0]);
    twoArrays[1] = nonFlags.toArray(new String[0]);

    return twoArrays;
  }

  /**
   * For an array master, returns
   * an array containing the last master.length-index elements.
   */
  public static String[] subarray(String[] master, int index) {
    if (master.length <= index) {
      return new String[0];
    } else {
      String[] sub = new String[master.length - index];
      System.arraycopy(master, index, sub, 0, sub.length);
      return sub;
    }
  }

  /**
   * Returns a string containing all the elements of args, space delimited.
   */
  public static String join(String[] args, String delimiter) {
    String output = "";
    StringBuilder builder = new StringBuilder();

    for (String arg : args) {
      if (builder.length() > 0) {
        builder.append(delimiter);
      }
      builder.append(arg);
    }

    return builder.toString();
  }

  public static String join(String[] args) {
    return join(args, " ");
  }

  public static String caps(String input) {
    if (input.length() == 0) {
      return input;
    }
    char first = Character.toUpperCase(input.charAt(0));
    return "" + first + input.substring(1);
  }

  public static String plural(String input) {
    return input + "s";
  }

  public static int compare(int one, int two) {
    return one - two;
  }

  public static int compare(long one, long two) {
    long result = one - two;

    if (result > 0) {
      return 1;
    }
    if (result < 0) {
      return -1;
    }
    return 0;
  }

  public static int compare(double one, double two) {
    double result = one - two;

    if (result > 0) {
      return 1;
    }
    if (result < 0) {
      return -1;
    }
    return 0;
  }

  public static int compare(float one, float two) {
    float result = one - two;

    if (result > 0) {
      return 1;
    }
    if (result < 0) {
      return -1;
    }
    return 0;
  }

  public static int compare(String one, String two) {
    return one.compareTo(two);
  }

  public static int compare(byte[] one, byte[] two) {
    int sharedLength = Math.min(one.length, two.length);

    for (int i = 0; i < sharedLength; i++) {
      int a = ((int) one[i]) & 0xFF;
      int b = ((int) two[i]) & 0xFF;
      int result = a - b;

      if (result < 0) {
        return -1;
      }
      if (result > 0) {
        return 1;
      }
    }

    return one.length - two.length;
  }

  public static int hash(byte b) {
    return ((int) b) & 0xFF;
  }

  public static int hash(int i) {
    return i;
  }

  public static int hash(long l) {
    return (int) l;
  }

  public static int hash(double d) {
    return (int) (d * 100000);
  }

  public static int hash(float f) {
    return (int) (f * 100000);
  }

  public static int hash(String s) {
    return s.hashCode();
  }

  public static int hash(byte[] b) {
    int h = 0;
    for (int i = 0; i < b.length; i++) {
      h += 7 * h + b[i];
    }
    return h;
  }

  public static void deleteDirectory(File directory) throws IOException {
    if (directory.isDirectory()) {
      for (File sub : directory.listFiles()) {
        if (sub.isDirectory()) {
          deleteDirectory(sub);
        } else {
          sub.delete();
        }
      }
    }
    directory.delete();
  }

  public static void partialDeleteDirectory(File directory, Set<String> omissions) throws IOException {
    for (File sub : directory.listFiles()) {
      if (omissions.contains(sub.getName())) {
        // don't delete this file
      } else if (sub.isDirectory()) {
        deleteDirectory(sub);
      } else {
        sub.delete();
      }
    }
    if (directory.listFiles().length == 0) {
      directory.delete();
    }
  }

  public static File createGalagoTempDir() throws IOException {
    return createGalagoTempDir("");
  }

  public static File createGalagoTempDir(String path) throws IOException {
    File galagoTemp = null;
    if (path.length() > 0) {
      galagoTemp = new File(path);
    } else {
      galagoTemp = Utility.createTemporary();
    }

    makeParentDirectories(galagoTemp.getAbsolutePath());
    galagoTemp.delete();
    galagoTemp.mkdir();

    return galagoTemp;
  }

  public static File createTemporary() throws IOException {
    return createTemporary(1024 * 1024 * 1024);
  }

  public static long getUnixFreeSpace(String pathname) throws IOException {
    try {
      // BUGBUG: will not work on windows
      String[] command = {"df", "-Pk", pathname};
      Process process = Runtime.getRuntime().exec(command);
      InputStream procOutput = process.getInputStream();
      BufferedReader reader = new BufferedReader(new InputStreamReader(procOutput));

      // skip the first line
      reader.readLine();
      String line = reader.readLine();
      String[] fields = line.split("\\s+");
      reader.close();

      process.getErrorStream().close();
      process.getInputStream().close();
      process.getOutputStream().close();
      process.waitFor();

      long freeSpace = Long.parseLong(fields[3]) * 1024;
      return freeSpace;
    } catch (InterruptedException ex) {
      return 0;
    }
  }

  public static long getFreeSpace(String pathname) throws IOException {
    try {
      // this will only work in Java 1.6 or later
      Method m = File.class.getMethod("getUsableSpace");
      Long result = (Long) m.invoke(new File(pathname));
      return (long) result;
    } catch (Exception e) {
      try {
        return getUnixFreeSpace(pathname);
      } catch (Exception ex) {
        return 1024 * 1024 * 1024; // 1GB
      }
    }
  }

  public static File createTemporary(long requiredSpace) throws IOException {
    File temporary;
    String root = getBestTemporaryLocation(requiredSpace);
    if (root != null) {
      temporary = File.createTempFile("tupleflow", "", new File(root));
    } else {
      temporary = File.createTempFile("tupleflow", "");
    }

    LOG.info("UTILITY_CREATED: " + temporary.getAbsolutePath());
    return temporary;
  }
  // So that we're not reading the file containing these OVER and OVER
  private static String[] roots;

  public static String getBestTemporaryLocation(long requiredSpace) throws IOException {
    for (String root : roots) {
      long freeSpace = getFreeSpace(root);

      if (freeSpace >= requiredSpace) {
        String logString = String.format("Found %6.3fMB >= %6.3fMB left on %s",
                freeSpace / 1048576.0, requiredSpace / 1048576.0, root);
        LOG.info(logString);
        return root;
      }
    }
    return null;
  }

  /**
   * Copies data from the input stream to the output stream.
   * @param input The input stream.
   * @param output The output stream.
   * @throws java.io.IOException
   */
  public static void copyStream(InputStream input, OutputStream output) throws IOException {
    byte[] data = new byte[65536];
    while (true) {
      int bytesRead = input.read(data);
      if (bytesRead < 0) {
        break;
      }
      output.write(data, 0, bytesRead);
    }
  }

  /**
   * Copies the data from file into the stream.  Note that this method
   * does not close the stream (in case you want to put more in it).
   *
   * @param file
   * @param stream
   * @throws java.io.IOException
   */
  public static void copyFileToStream(File file, OutputStream stream) throws IOException {
    FileInputStream input = new FileInputStream(file);
    long longLength = file.length();
    final int fiveMegabytes = 5 * 1024 * 1024;

    while (longLength > 0) {
      int chunk = (int) Math.min(longLength, fiveMegabytes);
      byte[] data = new byte[chunk];
      input.read(data, 0, chunk);
      stream.write(data, 0, chunk);
      longLength -= chunk;
    }

    input.close();
  }

  /**
   * Copies the data from the InputStream to a file, then closes both when
   * finished.
   *
   * @param stream
   * @param file
   * @throws java.io.IOException
   */
  public static void copyStreamToFile(InputStream stream, File file) throws IOException {
    FileOutputStream output = new FileOutputStream(file);
    final int oneMegabyte = 1 * 1024 * 1024;
    byte[] data = new byte[oneMegabyte];

    while (true) {
      int bytesRead = stream.read(data);

      if (bytesRead < 0) {
        break;
      }
      output.write(data, 0, bytesRead);
    }

    stream.close();
    output.close();
  }

  /**
   * Copies the data from the string s to the file.
   *
   * @param s
   * @param file
   * @throws java.io.IOException
   */
  public static void copyStringToFile(String s, File file) throws IOException {
    InputStream stream = new ByteArrayInputStream(Utility.fromString(s));
    Utility.copyStreamToFile(stream, file);
  }

  public static void calculateMessageDigest(File file, MessageDigest instance) throws IOException {
    FileInputStream input = new FileInputStream(file);
    final int oneMegabyte = 1024 * 1024;
    byte[] data = new byte[oneMegabyte];

    while (true) {
      int bytesRead = input.read(data);

      if (bytesRead < 0) {
        break;
      }
      instance.update(data, 0, bytesRead);
    }

    input.close();
  }

  public static HashSet<String> readStreamToStringSet(InputStream stream) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
    HashSet<String> set = new HashSet<String>();
    String line;

    while ((line = reader.readLine()) != null) {
      set.add(line.trim());
    }

    reader.close();
    return set;
  }

  public static HashSet<String> readFileToStringSet(File file) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(file));
    HashSet<String> set = new HashSet<String>();
    String line;

    while ((line = reader.readLine()) != null) {
      set.add(line.trim());
    }

    reader.close();
    return set;
  }

  // A workaround to make File versions of packaged resources. If it exists already, we return that and hope
  // it's what they wanted.
  // Note that we simply use the filename of the resource because, well, sometimes that's important when
  // poor coding is involved.
  public static File createResourceFile(Class requestingClass, String resourcePath) throws IOException {
    String tmpPath = getBestTemporaryLocation(1024 * 1024 * 100);
    if (tmpPath == null) {
      tmpPath = "";
    }

    String[] parts = resourcePath.split(File.separator);
    String fileName = parts[parts.length - 1];

    LOG.info(String.format("Creating resource file: %s/%s", tmpPath, fileName));
    File tmp = new File(tmpPath, fileName);
    if (tmp.exists()) {
      return tmp;
    }

    InputStream resourceStream = requestingClass.getResourceAsStream(resourcePath);
    if (resourceStream == null) {
      LOG.warning(String.format("Unable to create resource file."));
      return null;
    }

    copyStreamToFile(resourceStream, tmp);
    return tmp;
  }

  public static long skipLongBytes(DataInput in, long skipAmt) throws IOException {
    if (skipAmt < Integer.MAX_VALUE) {
      return in.skipBytes((int) skipAmt);
    }

    long skipped = 0;
    while (skipAmt >= Integer.MAX_VALUE) {
      skipped += in.skipBytes(Integer.MAX_VALUE);
      skipAmt -= Integer.MAX_VALUE;
    }
    skipped += in.skipBytes((int) skipAmt);
    return skipped;
  }

  /*
   * Functions to translate:
   *  - strings to bytes
   *  - bytes to strings
   *  - integers to bytes
   *  - bytes to integers
   */
  public static String toString(byte[] word) {
    try {
      return new String(word, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("UTF-8 is not supported by your Java Virtual Machine.");
    }
  }

  public static byte[] fromString(String word) {
    try {
      return word.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("UTF-8 is not supported by your Java Virtual Machine.");
    }
  }

  public static short toShort(byte[] key) {
    assert (key.length == 2);
    int value = ((key[0] << 8) + key[1]);
    return ((short) value);
  }

  public static byte[] fromShort(short key) {
    byte[] writeBuffer = new byte[2];
    writeBuffer[0] = (byte) ((key >>> 8) & 0xFF);
    writeBuffer[1] = (byte) key;
    return writeBuffer;
  }

  public static int toInt(byte[] key) {
    assert (key.length == 4);
    return (((key[0] & 255) << 24)
            + ((key[1] & 255) << 16)
            + ((key[2] & 255) << 8)
            + (key[3] & 255));
  }

  public static byte[] fromInt(int key) {
    byte[] converted = new byte[4];
    converted[0] = (byte) ((key >>> 24) & 0xFF);
    converted[1] = (byte) ((key >>> 16) & 0xFF);
    converted[2] = (byte) ((key >>> 8) & 0xFF);
    converted[3] = (byte) (key & 0xFF);
    return converted;
  }

  public static long toLong(byte[] key) {
    assert (key.length == 8);
    return (((long) key[0] << 56)
            + ((long) (key[1] & 255) << 48)
            + ((long) (key[2] & 255) << 40)
            + ((long) (key[3] & 255) << 32)
            + ((long) (key[4] & 255) << 24)
            + ((key[5] & 255) << 16)
            + ((key[6] & 255) << 8)
            + ((key[7] & 255) << 0));
  }

  public static byte[] fromLong(long key) {
    byte[] writeBuffer = new byte[8];

    writeBuffer[0] = (byte) (key >>> 56);
    writeBuffer[1] = (byte) (key >>> 48);
    writeBuffer[2] = (byte) (key >>> 40);
    writeBuffer[3] = (byte) (key >>> 32);
    writeBuffer[4] = (byte) (key >>> 24);
    writeBuffer[5] = (byte) (key >>> 16);
    writeBuffer[6] = (byte) (key >>> 8);
    writeBuffer[7] = (byte) (key >>> 0);
    return writeBuffer;
  }

  public static void compressInt(DataOutput output, int i) throws IOException {
    assert i >= 0;

    if (i < 1 << 7) {
      output.writeByte((i | 0x80));
    } else if (i < 1 << 14) {
      output.writeByte((i >> 0) & 0x7f);
      output.writeByte(((i >> 7) & 0x7f) | 0x80);
    } else if (i < 1 << 21) {
      output.writeByte((i >> 0) & 0x7f);
      output.writeByte((i >> 7) & 0x7f);
      output.writeByte(((i >> 14) & 0x7f) | 0x80);
    } else if (i < 1 << 28) {
      output.writeByte((i >> 0) & 0x7f);
      output.writeByte((i >> 7) & 0x7f);
      output.writeByte((i >> 14) & 0x7f);
      output.writeByte(((i >> 21) & 0x7f) | 0x80);
    } else {
      output.writeByte((i >> 0) & 0x7f);
      output.writeByte((i >> 7) & 0x7f);
      output.writeByte((i >> 14) & 0x7f);
      output.writeByte((i >> 21) & 0x7f);
      output.writeByte(((i >> 28) & 0x7f) | 0x80);
    }
  }

  public static int uncompressInt(DataInput input) throws IOException {
    int result = 0;
    int b;

    for (int position = 0; true; position++) {
      assert position < 6;
      b = input.readUnsignedByte();
      if ((b & 0x80) == 0x80) {
        result |= ((b & 0x7f) << (7 * position));
        break;
      } else {
        result |= (b << (7 * position));
      }
    }

    return result;
  }

  public static int uncompressInt(byte[] input, int offset) throws IOException {
    int result = 0;
    int b;

    for (int position = 0; true; position++) {
      assert input.length < 6;
      b = input[position + offset];
      if ((b & 0x80) == 0x80) {
        result |= ((b & 0x7f) << (7 * position));
        break;
      } else {
        result |= (b << (7 * position));
      }
    }

    return result;
  }

  public static void compressLong(DataOutput output, long i) throws IOException {
    assert i >= 0;

    if (i < 1 << 7) {
      output.writeByte((int) (i | 0x80));
    } else if (i < 1 << 14) {
      output.writeByte((int) (i >> 0) & 0x7f);
      output.writeByte((int) ((i >> 7) & 0x7f) | 0x80);
    } else if (i < 1 << 21) {
      output.writeByte((int) (i >> 0) & 0x7f);
      output.writeByte((int) (i >> 7) & 0x7f);
      output.writeByte((int) ((i >> 14) & 0x7f) | 0x80);
    } else {
      while (i >= 1 << 7) {
        output.writeByte((int) (i & 0x7f));
        i >>= 7;
      }

      output.writeByte((int) (i | 0x80));
    }
  }

  public static long uncompressLong(DataInput input) throws IOException {
    long result = 0;
    long b;

    for (int position = 0; true; position++) {
      assert position < 10;
      b = input.readUnsignedByte();

      if ((b & 0x80) == 0x80) {
        result |= ((long) (b & 0x7f) << (7 * position));
        break;
      } else {
        result |= ((long) b << (7 * position));
      }
    }

    return result;
  }

  public static long uncompressLong(byte[] input, int offset) throws IOException {
    long result = 0;
    long b;

    for (int position = 0; true; position++) {
      assert position < 10;
      b = input[position + offset];

      if ((b & 0x80) == 0x80) {
        result |= ((long) (b & 0x7f) << (7 * position));
        break;
      } else {
        result |= ((long) b << (7 * position));
      }
    }

    return result;
  }

  /*
   * The following methods are used to display bytes as strings
   */
  public static String binaryString(double d) {
    return binaryString(Double.doubleToLongBits(d));
  }

  public static String binaryString(long l) {
    StringBuilder sb = new StringBuilder();
    long mask = 0x8000000000000000L;
    while (mask != 0) {
      if ((mask & l) != 0) {
        sb.append("1");
      } else {
        sb.append("0");
      }
      mask >>>= 1;
    }
    return sb.toString();
  }

  public static String binaryString(int i) {
    StringBuilder sb = new StringBuilder();
    int mask = 0x80000000;
    while (mask != 0) {
      if ((mask & i) != 0) {
        sb.append("1");
      } else {
        sb.append("0");
      }
      mask >>>= 1;
    }
    return sb.toString();
  }

  public static String intsToString(int[] ints) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < ints.length; i++) {
      sb.append(ints[i]).append(" ");
    }
    return sb.toString();
  }

  public static String byteString(byte[] bytes) {
    return byteString(bytes, bytes.length);
  }

  public static String byteString(byte[] bytes, int l) {
    StringBuilder sb = new StringBuilder();
    for (int j = 0; j < l; j++) {
      int mask = 0x80;
      while (mask != 0) {
        sb.append(((((byte) mask) & bytes[j]) != 0) ? 1 : 0);
        mask >>>= 1;
      }
      sb.append(" ");
    }
    return sb.toString();
  }
}
