// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.mem;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.galagosearch.core.index.DocumentLengthsWriter;
import org.galagosearch.core.index.DocumentNameWriter;
import org.galagosearch.core.index.ExtentIndexWriter;
import org.galagosearch.core.index.ManifestWriter;
import org.galagosearch.core.index.PositionIndexWriter;
import org.galagosearch.core.index.corpus.DocumentIndexWriter;
import org.galagosearch.core.index.corpus.DocumentReader.DocumentIterator;
import org.galagosearch.core.retrieval.structured.ExtentIndexIterator;
import org.galagosearch.core.retrieval.structured.Extent;
import org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator;
import org.galagosearch.core.types.NumberWordPosition;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.core.types.NumberedExtent;
import org.galagosearch.core.util.ExtentArray;
import org.galagosearch.tupleflow.FakeParameters;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.types.XMLFragment;

public class FlushToDisk {

  private Logger logger = Logger.getLogger(FlushToDisk.class.toString());
  private MemoryIndex index;
  private String outputFolder;

  public void flushMemoryIndex(MemoryIndex index, String folder) throws IOException {
    flushMemoryIndex(index, folder, true);
  }

  public void flushMemoryIndex(MemoryIndex index, String folder, boolean threaded) throws IOException {
    this.index = index;
    this.outputFolder = folder;

    // first verify that there is at least one document
    //   and one term in the index
    if (index.getCollectionLength() < 1
            || index.getDocumentCount() < 1) {
      return;
    }

    if (new File(outputFolder).isDirectory()) {
      Utility.deleteDirectory(new File(outputFolder));
    }
    if (new File(outputFolder).isFile()) {
      new File(outputFolder).delete();
    }

    File parts = new File(outputFolder + File.separator + "parts");
    parts.mkdirs();

    // SWARM HACK //
    while (!parts.canRead()) {
      parts.mkdirs();
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // stupid hack to work on swarm
      }
    }

    if (threaded) {
      flushThreaded(outputFolder);
    } else {
      flushLocal(outputFolder);
    }
  }

  private void flushManifest(String manifestFile) throws IOException {
    Parameters parameters = new Parameters();
    parameters.add("filename", manifestFile);
    Processor<XMLFragment> writer = new ManifestWriter(new FakeParameters(parameters));
    writer.process(new XMLFragment("collectionLength", Long.toString(index.getCollectionLength())));
    writer.process(new XMLFragment("documentCount", Long.toString(index.getDocumentCount())));
    writer.process(new XMLFragment("documentNumberOffset", Long.toString(index.getDocumentNumberOffset())));

    writer.close();
  }

  private void flushNames(String namesFile) throws IOException {
    Parameters parameters = new Parameters();
    parameters.add("filename", namesFile);

    Processor<NumberedDocumentData> writer = new DocumentNameWriter(new FakeParameters(parameters));
    NumberedDocumentDataIterator iterator = index.getDocumentNamesIterator();
    do {
      NumberedDocumentData ndd = iterator.getDocumentData();
      writer.process(ndd);
    } while (iterator.nextRecord());
    writer.close();
  }

  private void flushLengths(String namesFile) throws IOException {
    int offset = index.getDocumentNumberOffset();

    Parameters parameters = new Parameters();
    parameters.add("filename", namesFile);
    parameters.add("documentNumberOffset", Integer.toString(offset));

    Processor<NumberedDocumentData> writer = new DocumentLengthsWriter(new FakeParameters(parameters));
    NumberedDocumentDataIterator iterator = index.getDocumentLengthsIterator();
    do {
      NumberedDocumentData ndd = iterator.getDocumentData();
      writer.process(ndd);
    } while (iterator.nextRecord());
    writer.close();
  }

  private void flushExtents(ExtentIndexIterator iterator, String extentsFile) throws IOException {
    Parameters parameters = new Parameters();
    parameters.add("filename", extentsFile);

    NumberedExtent.ExtentNameNumberBeginOrder.ShreddedProcessor writer = new ExtentIndexWriter(new FakeParameters(parameters));
    NumberedExtent.ExtentNameNumberBeginOrder.TupleShredder processor = new NumberedExtent.ExtentNameNumberBeginOrder.TupleShredder(writer);
    if (!iterator.isDone()) {
      do {
        String extentName = iterator.getKey();
        int document = iterator.document();
        ExtentArray extents = iterator.extents();

        for (Extent extent : extents.toArray()) {
          NumberedExtent ne = new NumberedExtent(Utility.fromString(extentName), extent.document, extent.begin, extent.end);
          processor.process(ne);
        }

      } while (iterator.nextRecord());
    }
    processor.close();
  }

  private void flushPostings(ExtentIndexIterator iterator, String postingFile) throws IOException {
    Parameters parameters = new Parameters();
    parameters.add("filename", postingFile);

    NumberWordPosition.WordDocumentPositionOrder.ShreddedProcessor writer = new PositionIndexWriter(new FakeParameters(parameters));
    NumberWordPosition.WordDocumentPositionOrder.TupleShredder processor = new NumberWordPosition.WordDocumentPositionOrder.TupleShredder(writer);
    do {
      String extentName = iterator.getKey();
      int document = iterator.document();
      ExtentArray extents = iterator.extents();

      for (Extent extent : extents.toArray()) {
        NumberWordPosition nwp = new NumberWordPosition(extent.document, Utility.fromString(extentName), extent.begin);
        processor.process(nwp);
      }

    } while (iterator.nextRecord());
    processor.close();
  }

  private void flushCorpusData(DocumentIterator iterator, String corpusFile) throws IOException {
    Parameters parameters = new Parameters();
    parameters.add("filename", corpusFile);

    DocumentIndexWriter writer = new DocumentIndexWriter(new FakeParameters(parameters));
    do {
      writer.process(iterator.getDocument());
    } while (iterator.nextDocument());
    writer.close();
  }

  private void flushLocal(String outputFolder) throws IOException {

    // flush manifest
    flushManifest(outputFolder + File.separator + "manifest");
    // flush lengths
    flushLengths(outputFolder + File.separator + "documentLengths");
    // flush names
    flushNames(outputFolder + File.separator + "documentNames");
    // flush extents
    flushExtents(index.getExtentIterator("extents"), outputFolder + File.separator + "parts" + File.separator + "extents");
    // flush postings
    flushPostings(index.getExtentIterator("postings"), outputFolder + File.separator + "parts" + File.separator + "postings");
    // flush stemmed
    if (index.stemming) {
      flushPostings(index.getExtentIterator("stemmedPostings"), outputFolder + File.separator + "parts" + File.separator + "stemmedPostings");
    }
    // flush corpus
    if (index.makecorpus) {
      System.err.println("<<<FLUSHING CORPUS DATA>>>");
      flushCorpusData(index.getDocumentIterator(), outputFolder + File.separator + "corpus");
    } else {
       System.err.println(">>>> NOT FLUSHING CORPUS DATA <<<<");
   }
  }

  private void flushThreaded(String outputFolder) throws IOException {
    ArrayList<Thread> threads = getThreads(outputFolder);
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException ex) {
        logger.severe(ex.getMessage());
      }
    }

  }

  private ArrayList<Thread> getThreads(final String outputFolder) {
    ArrayList<Thread> threads = new ArrayList();

    final FlushToDisk f = this;

    // flush manifest
    threads.add(new Thread() {

      @Override
      public void run() {
        try {
          f.flushManifest(outputFolder + File.separator + "manifest");
        } catch (IOException e) {
          logger.severe(e.toString());
        }
      }
    });

    // flush lengths
    threads.add(new Thread() {

      @Override
      public void run() {
        try {
          f.flushLengths(outputFolder + File.separator + "documentLengths");
        } catch (IOException e) {
          logger.severe(e.toString());
        }
      }
    });

    // flush names
    threads.add(new Thread() {

      @Override
      public void run() {
        try {
          f.flushNames(outputFolder + File.separator + "documentNames");
        } catch (IOException e) {
          logger.severe(e.toString());
        }
      }
    });

    // flush extents
    threads.add(new Thread() {

      @Override
      public void run() {
        try {
          f.flushExtents(index.getExtentIterator("extents"), outputFolder + File.separator + "parts" + File.separator + "extents");
        } catch (IOException e) {
          logger.severe(e.toString());
        }
      }
    });

    // flush postings
    threads.add(new Thread() {

      @Override
      public void run() {
        try {
          f.flushPostings(index.getExtentIterator("postings"), outputFolder + File.separator + "parts" + File.separator + "postings");
        } catch (IOException e) {
          logger.severe(e.toString());
        }
      }
    });

    // flush stemmed
    if (index.stemming) {
      threads.add(new Thread() {

        @Override
        public void run() {
          try {
            f.flushPostings(index.getExtentIterator("stemmedPostings"), outputFolder + File.separator + "parts" + File.separator + "stemmedPostings");
          } catch (IOException e) {
            logger.severe(e.toString());
          }
        }
      });
    }

    if (index.makecorpus) {
      threads.add(new Thread() {

        @Override
        public void run() {
          try {
            f.flushCorpusData(index.getDocumentIterator(), outputFolder + File.separator + "corpus");
          } catch (IOException e) {
            logger.severe(e.toString());
          }
        }
      });
    }

    return threads;
  }
}
