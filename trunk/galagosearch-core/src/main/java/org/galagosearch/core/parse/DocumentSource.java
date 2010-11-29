// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.parse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.galagosearch.core.index.VocabularyReader;
import org.galagosearch.core.index.VocabularyReader.TermSlot;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.tupleflow.ExNihiloSource;
import org.galagosearch.tupleflow.FileSource;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verified;
import org.galagosearch.core.types.DocumentSplit;

/**
 * From a set of inputs, splits the input into many DocumentSplit records.
 * This will usually be in a stage by itself at the beginning of a Galago pipeline.
 * This is somewhat similar to FileSource, except that it can autodetect file formats.
 * This splitter can detect ARC, TREC, TRECWEB and corpus files.
 * 
 * @author trevor, sjh
 */
@Verified
@OutputClass(className = "org.galagosearch.core.types.DocumentSplit")
public class DocumentSource implements ExNihiloSource<DocumentSplit> {

  public Processor<DocumentSplit> processor;
  TupleFlowParameters parameters;
  int fileId = 0;
  int totalFileCount = 0;
  boolean emitSplits;

  public DocumentSource(TupleFlowParameters parameters) {
    this.parameters = parameters;
  }

  private String getExtension(String fileName) {
    String[] fields = fileName.split("\\.");

    // A filename needs to have a period to have an extension.
    if (fields.length <= 1) {
      return "";
    }

    // If the last chunk of the filename is gz, we'll ignore it.
    // The second-to-last bit is the type extension (but only if
    // there are at least three parts to the name).
    if (fields[fields.length - 1].equals("gz")) {
      if (fields.length > 2) {
        return fields[fields.length - 2];
      } else {
        return "";
      }
    }

    // Do the same thing w/ bz2 as above (MAC)
    if (fields[fields.length - 1].equals("bz2")) {
      if (fields.length > 2) {
        return fields[fields.length - 2];
      } else {
        return "";
      }
    }

    // No 'gz' extension, so just return the last part.
    return fields[fields.length - 1];
  }

  private void processCorpusFile(String fileName, String fileType) throws IOException {

    // we want to divde the corpus up into ~100MB chunks
    long chunkSize = 100 * 1024 * 1024;
    long corpusSize = 0L;

    if (CorpusReader.isCorpus(fileName)) {
      File folder = new File(fileName).getParentFile();
      for (File f : folder.listFiles()) {
        corpusSize += f.length();
      }
    } else { // must be a corpus file.
      corpusSize = new File(fileName).length();
    }


    IndexReader reader = new IndexReader(fileName);
    VocabularyReader vocabulary = reader.getVocabulary();
    List<TermSlot> slots = vocabulary.getSlots();
    int pieces = Math.max(2, (int) (corpusSize / chunkSize));
    ArrayList<byte[]> keys = new ArrayList<byte[]>();

    for (int i = 1; i < pieces; ++i) {
      float fraction = (float) i / pieces;
      int slot = (int) (fraction * slots.size());
      keys.add(slots.get(slot).termData);
    }

    for (int i = 0; i < pieces; ++i) {
      byte[] firstKey = new byte[0];
      byte[] lastKey = new byte[0];

      if (i > 0) {
        firstKey = keys.get(i - 1);
      }
      if (i < pieces - 1) {
        lastKey = keys.get(i);
      }

      if (Utility.compare(firstKey, lastKey) != 0) {
        DocumentSplit split = new DocumentSplit(fileName, fileType, false, firstKey, lastKey, fileId, totalFileCount);
        fileId++;
        if (emitSplits) {
          processor.process(split);
        }
      }
    }
  }

  private void processFile(String fileName) throws IOException {
    // First, try to detect what kind of file this is:
    boolean isCompressed = (fileName.endsWith(".gz") || fileName.endsWith(".bz2"));
    String fileType = null;

    // We'll try to detect by extension first, so we don't have to open the file
    String extension = getExtension(fileName);

    /**
     * This is a list file, meaning we need to iterate over its contents to
     * retrieve the file list. 
     *
     * Assumptions: Each line in this file should be a filename, NOT a directory.
     *              List file is either uncompressed or compressed using gzip ONLY.
     */
    if (extension.equals("list")) {
      BufferedReader br;
      if (fileName.endsWith("gz")) {
        br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(fileName))));
      } else {
        br = new BufferedReader(new FileReader(fileName));
      }

      while (br.ready()) {
        String entry = br.readLine().trim();
        if (entry.length() == 0) {
          continue;
        }
        processFile(entry);
      }
      br.close();
      return; // No more to do here -- this file is now "processed"
    }


    if (extension.equals("corpus")
            || extension.equals("trecweb")
            || extension.equals("trectext")
            || extension.equals("twitter")
            || extension.equals("warc")
            || extension.equals("arc")
            || extension.equals("txt")
            || extension.equals("html")
            || extension.equals("xml")) {
      fileType = extension;
    } else {
      // Oh well, we need to autodetect the file type.
      if (fileName.endsWith(".cds.z") || fileName.endsWith(".cds")) {
        // do nothing: file is a corpus document store
        return;
      } else if (IndexReader.isIndexFile(fileName)) {
        // perhaps the user has renamed the corpus index
        fileType = "corpus";
      } else {
        fileType = detectTrecTextOrWeb(fileName);

        // Eventually it'd be nice to do more format detection here.
        if (fileType == null) {
          System.err.println("Skipping: " + fileName);
          return;
        }
      }
    }

    if (fileType.equals("corpus")) {
      processCorpusFile(fileName, fileType);
    } else {
      processSplit(fileName, fileType, isCompressed);
    }
  }

  private void processSplit(String fileName, String fileType, boolean isCompressed) throws IOException {
    DocumentSplit split = new DocumentSplit(fileName, fileType, isCompressed, new byte[0], new byte[0], fileId, totalFileCount);
    fileId++;
    if (emitSplits) {
      processor.process(split);
    }
  }

  private void processDirectory(File root) throws IOException {
    for (File file : root.listFiles()) {
      if (file.isHidden()) {
        continue;
      }
      if (file.isDirectory()) {
        processDirectory(file);
      } else {
        processFile(file.getAbsolutePath());
      }
    }
  }

  public void run() throws IOException {
    // first count the total number of files
    emitSplits = false;
    if (parameters.getXML().containsKey("directory")) {
      List<Value> directories = parameters.getXML().list("directory");

      for (Value directory : directories) {
        File directoryFile = new File(directory.toString());
        processDirectory(directoryFile);
      }
    }
    if (parameters.getXML().containsKey("filename")) {
      List<Value> files = parameters.getXML().list("filename");

      for (Value file : files) {
        processFile(file.toString());
      }
    }

    // we now have an accurate count of emitted files / splits
    totalFileCount = fileId;
    fileId = 0; // reset to enumerate splits

    // now process each file
    emitSplits = true;
    if (parameters.getXML().containsKey("directory")) {
      List<Value> directories = parameters.getXML().list("directory");

      for (Value directory : directories) {
        File directoryFile = new File(directory.toString());
        processDirectory(directoryFile);
      }
    } else if (parameters.getXML().containsKey("filename")) {
      List<Value> files = parameters.getXML().list("filename");

      for (Value file : files) {
        processFile(file.toString());
      }
    }

    processor.close();
  }

  // For now we assume <doc> tags, so we read in one doc
  // (i.e. <doc> to </doc>), and look for the following
  // tags: <docno> and (<text> or <html>)
  private String detectTrecTextOrWeb(String fileName) {

    try {
      BufferedReader br = new BufferedReader(new FileReader(fileName));
      String line;

      // check the first ten lines for a "<doc>" line
      //  - as file could have some header data
      boolean docflag = false;
      for (int i = 0; i < 10; i++) {
        line = br.readLine();
        if (line != null
                && line.equalsIgnoreCase("<doc>")) {
          docflag = true;
        }
      }
      if (!docflag) {
        return null;
      }

      // Now just read until we see docno and (text or html) tags
      boolean hasDocno, hasHtml, hasText;
      hasDocno = hasHtml = hasText = false;
      String fileType = null;
      while (br.ready()) {
        line = br.readLine();
        if (line == null || line.equalsIgnoreCase("</doc>")) {
          break; // doc is closed or null line
        }
        line = line.toLowerCase();
        if (line.indexOf("<docno>") != -1) {
          hasDocno = true;
        } else if (line.indexOf("<text>") != -1) {
          hasText = true;
        } else if (line.indexOf("<html>") != -1) {
          hasHtml = true;
        }

        if (hasDocno && hasText) {
          fileType = "trectext";
          break;
        } else if (hasDocno && hasHtml) {
          fileType = "trecweb";
          break;
        }
      }
      br.close();

      return fileType;
    } catch (IOException ioe) {
      ioe.printStackTrace(System.err);
      return null;
    }
  }

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    FileSource.verify(parameters, handler);
  }
}
