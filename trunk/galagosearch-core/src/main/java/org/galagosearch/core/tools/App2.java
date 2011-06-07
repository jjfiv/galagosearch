// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import org.galagosearch.core.index.DocumentNameReader;
import org.galagosearch.core.index.KeyIterator;
import org.galagosearch.core.index.KeyListReader;
import org.galagosearch.core.index.KeyValueReader;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.index.StructuredIndexPartModifier;
import org.galagosearch.core.index.StructuredIndexPartReader;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.core.index.corpus.DocumentReader;
import org.galagosearch.core.index.corpus.DocumentReader.DocumentIterator;
import org.galagosearch.core.index.merge.MergeIndexes;
import org.galagosearch.core.parse.Document;
import org.galagosearch.tupleflow.FileOrderedReader;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorStore;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.execution.JobExecutor;
import org.mortbay.jetty.Server;

/**
 * @author sjh, irmarc, trevor
 */
public class App2 {

  // this interface for each function
  public interface AppFunction {

    public String getHelpString();

    public void run(String[] args, PrintStream output) throws Exception;
  }

  /*
   * Main function
   */
  public static void main(String[] args) throws Exception {
    App2 app = new App2(System.out);
    app.run(args);
  }
  /** 
   * function selection and processing
   */
  protected HashMap<String, AppFunction> appFunctions = new HashMap();

  public App2(PrintStream out) {

    // build functions
    appFunctions.put("build", new BuildFn());
    appFunctions.put("build-fast", new BuildFn());
    appFunctions.put("build-special", new BuildSpecialFn());
    appFunctions.put("build-topdocs", new BuildTopDocsFn());
    appFunctions.put("build-window", new BuildWindowFn());
    appFunctions.put("make-corpus", new MakeCorpusFn());
    appFunctions.put("merge-index", new MergeIndexFn());

    // search functions
    appFunctions.put("batch-search", new BatchSearchFn());
    appFunctions.put("search", new SearchFn());

    // eval 
    appFunctions.put("eval", new EvalFn());

    // dump functions
    appFunctions.put("dump-connection", new DumpConnectionFn());
    appFunctions.put("dump-corpus", new DumpCorpusFn());
    appFunctions.put("dump-index", new DumpIndexFn());
    appFunctions.put("dump-keys", new DumpKeysFn());
    appFunctions.put("dump-keyvalue", new DumpKeyValueFn()); // -- should be implemented in dump-index
    appFunctions.put("dump-modifier", new DumpModifierFn());

    // corpus + index querying
    appFunctions.put("doc", new DocFn());
    appFunctions.put("doc-id", new DocIdFn());
    appFunctions.put("xcount", new XCountFn());
    appFunctions.put("doccount", new XDocCountFn());

    // help function
    appFunctions.put("help", new HelpFn());

  }

  public void run(String[] args) throws Exception {
    String fn = "help";
    if (args.length > 0) {
      fn = args[0];
    }
    appFunctions.get(fn).run(args, System.out);
  }

  /* 
   * Function implementations - in alphbetical order
   */
  private class BatchSearchFn implements AppFunction {

    public String getHelpString() {
      return "galago batch-search <args>\n\n"
              + "  Runs a batch of queries against an index and produces TREC-formatted\n"
              + "  output.  The output can be used with retrieval evaluation tools like\n"
              + "  galago eval (org.galagosearch.core.eval).\n\n"
              + "  Sample invocation:\n"
              + "     galago batch-search --index=/tmp/myindex --count=200 /tmp/queries\n\n"
              + "  Args:\n"
              + "     --index=path_to_your_index\n"
              + "     --count : Number of results to return for each query, default=1000\n"
              + "     /path/to/parameter/file : Input file in xml parameters format (see below).\n\n"
              + "  Query file format:\n"
              + "    The query file is an XML file containing a set of queries.  Each query\n"
              + "    has text tag, which contains the text of the query, and a number tag, \n"
              + "    which uniquely identifies the query in the output.\n\n"
              + "  Example query file:\n"
              + "  <parameters>\n"
              + "     <query>\n"
              + "        <number>CACM-408</number>\n"
              + "        <text>#combine(my query)</text>\n"
              + "     </query>\n"
              + "     <query>\n"
              + "        <number>WIKI-410</number>\n"
              + "        <text>#combine(another query)</text>\n"
              + "     </query>\n"
              + "  </parameters>\n";
    }

    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 1) {
        output.println(getHelpString());
        return;
      }

      BatchSearch.run(Utility.subarray(args, 1), output);
    }
  }

  private class BuildFn implements AppFunction {

    public String getHelpString() {
      return "galago build[-fast] [flags] --indexPath=<index> (--inputPaths=<input>)+\n\n"
              + "  Builds a Galago StructuredIndex with TupleFlow, using one thread\n"
              + "  for each CPU core on your computer.  While some debugging output\n"
              + "  will be displayed on the screen, most of the status information will\n"
              + "  appear on a web page.  A URL should appear in the command output\n"
              + "  that will direct you to the status page.\n\n"
              + "<input>:  Can be either a file or directory, and as many can be\n"
              + "          specified as you like.  Galago can read html, xml, txt, \n"
              + "          arc (Heritrix), warc, trectext, trecweb and corpus files.\n"
              + "          Files may be gzip compressed (.gz|.bz).\n"
              + "<index>:  The directory path of the index to produce.\n\n"
              + "Algorithm Flags:\n"
              + "  --links={true|false}:    Selects whether to collect anchor text\n"
              + "                           [default=false]\n"
              + "  --stemming={true|false}: Selects whether to build stemmed inverted \n"
              + "                           lists in addition to non-stemmed ones.\n"
              + "                           [default=true]\n"
              + "  --corpusPath=/path/for/corpus: Selects the location to output a corpus folder.\n"
              + "                           Note that this is optional, if no path is supplied,\n"
              + "                           then no corpus will be created.\n"
              + "                           [default=None]\n\n"
              + "Tupleflow Flags:\n"
              + "  --printJob={true|false}: Simply prints the execution plan of a Tupleflow-based job then exits.\n"
              + "                           [default=false]\n"
              + "  --mode={local|threaded|drmaa}: Selects which executor to use \n"
              + "                           [default=local]\n"
              + "  --port={int<65000} :     port number for web based progress monitoring. \n"
              + "                           [default=randomly selected free port]\n"
              + "  --galagoTemp=/path/to/temp/dir/: Sets the galago temp dir \n"
              + "                           [default = uses folders specified in ~/.galagotmp or java.io.tmpdir]\n"
              + "  --deleteOutput={0|1|2}:    Selects how much of the galago temp dir to delete\n"
              + "                           0 --> keep all data\n"
              + "                           1 --> delete all data + keep jobs directory (only useful for drmaa mode)\n"
              + "                           2 --> delete entire temp directory\n"
              + "                           [default=2]\n"
              + "  --distrib={int > 1}:     Selects the number of simultaneous jobs to create\n"
              + "                           [default = 10]\n";
    }

    public void run(String[] args, PrintStream output) throws Exception {
      Parameters p = new Parameters(Utility.subarray(args, 1));
      p.set("command", Utility.join(args, " "));

      // build-fast index input
      if (!p.containsKey("indexPath")
              || (p.list("indexPath").size() > 1)
              || !p.containsKey("inputPaths")) {
        output.println(getHelpString());
        return;
      }

      Job job;
      if (args[0].contains("fast")) {
        BuildFastIndex build = new BuildFastIndex();
        job = build.getIndexJob(p);
      } else {
        BuildIndex build = new BuildIndex();
        job = build.getIndexJob(p);
      }

      runTupleFlowJob(job, p, output);
    }
  }

  private class BuildSpecialFn implements AppFunction {

    public String getHelpString() {
      return "galago build-special [flags] --indexPath=<index> (--inputPaths=<input>)+\n\n"
              + "  Builds a Galago Structured Index Part file with TupleFlow,\n"
              + "  Can build either an indicator part or prior part.\n\n"
              + "<indicator-input>:  One or more indicator files in format:\n"
              + "           < document-identifier \t [true | false] >\n\n"
              + "<prior-input>:  One or more indicator files in format:\n"
              + "           < document-identifier \t [log-probability] >\n\n"
              + "<index>:  The directory path of the index to add to.\n\n"
              + "Algorithm Flags:\n"
              + "  --type={indicator|prior}: Sets the type of index part to build.\n"
              + "                            [default=prior]\n\n"
              + "  --partName={String}:      Sets the name of index part.\n"
              + "                 indicator: [default=prior]\n"
              + "                     prior: [default=indicator]\n"
              + "  --extractor={java class}: Sets the class that extracts boolean values for each input line.\n"
              + "                 indicator: [default=org.galagosearch.core.parse.IndicatorExtractor]\n"
              + "                     prior: [default=org.galagosearch.core.parse.PriorExtractor]\n\n"
              + "  --default={true|false|float}: Sets the default value for the index part.\n"
              + "                 indicator: [default=false]\n"
              + "                     prior: [default=-inf\n\n"
              + "  --priorType={raw|prob|logprob}: Sets the type of prior to read. (Only for prior parts)\n"
              + "                            [default=raw]\n\n"
              + "Tupleflow Flags:\n"
              + "  --printJob={true|false}: Simply prints the execution plan of a Tupleflow-based job then exits.\n"
              + "                           [default=false]\n"
              + "  --mode={local|threaded|drmaa}: Selects which executor to use \n"
              + "                           [default=local]\n"
              + "  --port={int<65000} :     port number for web based progress monitoring. \n"
              + "                           [default=randomly selected free port]\n"
              + "  --galagoTemp=/path/to/temp/dir/: Sets the galago temp dir \n"
              + "                           [default = uses folders specified in ~/.galagotmp or java.io.tmpdir]\n"
              + "  --deleteOutput={0|1|2}:    Selects how much of the galago temp dir to delete\n"
              + "                           0 --> keep all data\n"
              + "                           1 --> delete all data + keep jobs directory (only useful for drmaa mode)\n"
              + "                           2 --> delete entire temp directory\n"
              + "                           [default=2]\n"
              + "  --distrib={int > 1}:     Selects the number of simultaneous jobs to create\n"
              + "                           [default = 10]\n";
    }

    public void run(String[] args, PrintStream output) throws Exception {
      Parameters p = new Parameters(args);
      p.set("command", Utility.join(args, " "));

      if (!p.containsKey("indexPath")
              || (p.list("indexPath").size() > 1)
              || !p.containsKey("inputPaths")) {
        output.println(getHelpString());
        return;
      }

      Job job = null;
      BuildSpecialPart build = new BuildSpecialPart();
      String type = p.get("type", "prior");
      if (type.equals("indicator")) {
        job = build.getIndicatorJob(p);
      } else if (type.equals("prior")) {
        job = build.getPriorJob(p);
      }

      runTupleFlowJob(job, p, output);
    }
  }

  private class BuildTopDocsFn implements AppFunction {

    public String getHelpString() {
      return "galago build-topdocs <index> <part> <size> <minlength>\n\n"
              + "  Constructs topdoc lists consisting of <size> documents,\n"
              + "  and only for lists longer than <minlength>. Note that\n"
              + "  <index> needs to point an index, while <part> is the part to scan.\n";
    }

    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length < 5) {
        output.println(getHelpString());
        return;
      }

      String[][] filtered = Utility.filterFlags(args);
      String[] flags = filtered[0];
      String[] nonFlags = Utility.subarray(filtered[1], 1);

      Parameters toJob = new Parameters();
      toJob.set("index", nonFlags[0]);
      toJob.set("part", nonFlags[1]);
      toJob.set("size", nonFlags[2]);
      toJob.set("minlength", nonFlags[3]);

      Job job;
      BuildTopDocs build = new BuildTopDocs();
      job = build.getIndexJob(toJob);

      Parameters toServer = new Parameters(flags);
      toServer.set("command", Utility.join(args, " "));

      runTupleFlowJob(job, toServer, output);
    }
  }

  private class BuildWindowFn implements AppFunction {

    public String getHelpString() {
      return "galago window [flags] --indexPath=<index> (--inputPaths=<input>)+\n\n"
              + "  Builds a Galago StructuredIndex window index file using TupleFlow. Program\n"
              + "  uses one thread for each CPU core on your computer.  While some debugging output\n"
              + "  will be displayed on the screen, most of the status information will\n"
              + "  appear on a web page.  A URL should appear in the command output \n"
              + "  that will direct you to the status page.\n\n"
              + "  Arg: --spaceEfficient=true will produce an identical window index using "
              + "  a two-pass space efficient algorithm. \n\n"
              + "  Ordered or unordered windows can be generated. We match the #od and\n"
              + "  #uw operator definitions (See galago query language). Width of an ordered window\n"
              + "  is the maximum distance between words. Width of an unordered window is\n"
              + "  the differencebetween the location of the last word and the location of \n"
              + "  the first word.\n\n"
              + "  <input>:  Can be either a file or directory, and as many can be\n"
              + "          specified as you like.  Galago can read html, xml, txt, \n"
              + "          arc (Heritrix), trectext, trecweb and corpus files.\n"
              + "          Files may be gzip compressed (.gz).\n"
              + "  <index>:  The directory path of the existing index (over the same corpus).\n\n"
              + "Algorithm Flags:\n"
              + "  --n={int >= 2}:          Selects the number of terms in each window (any reasonable value is possible).\n"
              + "                           [default = 2]\n"
              + "  --width={int >= 1}:      Selects the width of the window (Note: ordered windows are different to unordered windows).\n"
              + "                           [default = 1]\n"
              + "  --ordered={true|false}:  Selects ordered or unordered windows.\n"
              + "                           [default = true]\n"
              + "  --threshold={int >= 1}:  Selects the minimum number length of any inverted list.\n"
              + "                           Larger values will produce smaller indexes.\n"
              + "                           [default = 2]\n"
              + "  --usedocfreq={true|false}: Determines if the threshold is applied to term freq or doc freq.\n"
              + "                           [default = false]\n"
              + "  --stemming={true|false}: Selects whether to build a stemmed ngram inverted list.\n"
              + "                           [default=false]\n\n"
              + "  --spaceEfficient={true|false}: Selects whether to use a space efficient algorithm.\n"
              + "                           (The cost is an extra pass over the input data).\n"
              + "                           [default=false]\n\n"
              + "Tupleflow Flags:\n"
              + "  --printJob={true|false}: Simply prints the execution plan of a Tupleflow-based job then exits.\n"
              + "                           [default=false]\n"
              + "  --mode={local|threaded|drmaa}: Selects which executor to use \n"
              + "                           [default=local]\n"
              + "  --port={int<65000} :     port number for web based progress monitoring. \n"
              + "                           [default=randomly selected free port]\n"
              + "  --galagoTemp=/path/to/temp/dir/: Sets the galago temp dir \n"
              + "                           [default = uses folders specified in ~/.galagotmp or java.io.tmpdir]\n"
              + "  --deleteOutput={0|1|2}:    Selects how much of the galago temp dir to delete\n"
              + "                           0 --> keep all data\n"
              + "                           1 --> delete all data + keep jobs directory (only useful for drmaa mode)\n"
              + "                           2 --> delete entire temp directory\n"
              + "                           [default=2]\n"
              + "  --distrib={int > 1}:     Selects the number of simultaneous jobs to create\n"
              + "                           [default = 10]\n";
    }

    public void run(String[] args, PrintStream output) throws Exception {
      Parameters p = new Parameters(Utility.subarray(args, 1));
      p.set("command", Utility.join(args, " "));

      // build-fast index input
      if (!p.containsKey("indexPath")
              || (p.list("indexPath").size() > 1)
              || !p.containsKey("inputPaths")) {
        output.println(getHelpString());
        return;
      }

      Job job;
      BuildWindowIndex build = new BuildWindowIndex();
      job = build.getIndexJob(p);

      runTupleFlowJob(job, p, output);
    }
  }

  private class DocFn implements AppFunction {

    public String getHelpString() {
      return "galago doc <corpus> <identifier>\n\n"
              + "  Prints the full text of the document named by <identifier>.\n"
              + "  The document is retrieved from a Corpus file named <corpus>.";
    }

    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 2) {
        output.println(getHelpString());
        return;
      }

      String indexPath = args[1];
      String identifier = args[2];
      DocumentReader reader = DocumentReader.getInstance(indexPath);

      Document document = reader.getDocument(identifier);
      output.println(document.text);
    }
  }

  private class DocIdFn implements AppFunction {

    public String getHelpString() {
      return "Two possible use cases:\n\n"
              + "galago doc-id <index> <internal-number>\n"
              + "  Prints the external document identifier of the document <internal-number>.\n\n"
              + "galago doc-id <documentNames.ReverseLookup> <identifier>\n"
              + "  Prints the internal document number of the document named by <identifier>.\n";
    }

    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 2) {
        output.println(getHelpString());
        return;
      }

      String indexPath = args[1];
      String id = args[2];

      DocumentNameReader reader = new DocumentNameReader(indexPath);

      if (reader.isForward) {
        String docIdentifier = reader.get(Integer.parseInt(id));
        output.println(docIdentifier);
      } else {
        int docNum = reader.getDocumentId(id);
        output.println(docNum);
      }
    }
  }

  private class DumpConnectionFn implements AppFunction {

    public String getHelpString() {
      return "galago dump-connection <connection-file>\n\n"
              + "  Dumps tuples from a Galago TupleFlow connection file in \n"
              + "  CSV format.  This can be useful for debugging strange problems \n"
              + "  in a TupleFlow execution.\n";
    }

    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 1) {
        output.println(getHelpString());
        return;
      }

      FileOrderedReader reader = new FileOrderedReader(args[1]);
      Object o;
      while ((o = reader.read()) != null) {
        output.println(o);
      }
    }
  }

  private class DumpCorpusFn implements AppFunction {

    public String getHelpString() {
      return "galago dump-corpus <corpus>\n\n"
              + "  Dumps all documents from a corpus file to stdout.\n";
    }

    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 1) {
        output.println(getHelpString());
        return;
      }

      DocumentReader reader = DocumentReader.getInstance(args[1]);
      DocumentReader.DocumentIterator iterator = (DocumentIterator) reader.getIterator();

      while (!iterator.isDone()) {
        output.println("#IDENTIFIER: " + iterator.getKey());
        Document document = iterator.getDocument();
        output.println("#METADATA");
        for (Entry<String, String> entry : document.metadata.entrySet()) {
          output.println(entry.getKey() + "," + entry.getValue());
        }
        output.println("#TEXT");
        output.println(document.text);
        iterator.nextKey();
      }
      reader.close();
    }
  }

  private class DumpIndexFn implements AppFunction {

    public String getHelpString() {
      return "galago dump-index <index-part>\n\n"
              + "  Dumps inverted list data from any index file in a StructuredIndex\n"
              + "  (That is, any index that has a readerClass that's a subclass of\n"
              + "  StructuredIndexPartReader).  Output is in CSV format.\n";
    }

    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 1) {
        output.println(getHelpString());
        return;
      }

      StructuredIndexPartReader reader = StructuredIndex.openIndexPart(args[1]);
      KeyIterator iterator = reader.getIterator();

      // if we have a key-list index
      if (KeyListReader.class.isAssignableFrom(reader.getClass())) {
        while (!iterator.isDone()) {
          ValueIterator vIter = iterator.getValueIterator();
          while (!vIter.isDone()) {
            output.println(vIter.getEntry());
            vIter.next();
          }
          iterator.nextKey();
        }

        // otherwise we could have a key-value index
      } else if (KeyValueReader.class.isAssignableFrom(reader.getClass())) {
        while (!iterator.isDone()) {
          output.println(iterator.getKey() + "," + iterator.getValueString());
          iterator.nextKey();
        }
      } else {
        output.println("Unable to read index as a key-list or a key-value reader.");
      }

      reader.close();
    }
  }

  private class DumpKeysFn implements AppFunction {

    public String getHelpString() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 1) {
        output.println(getHelpString());
        return;
      }

      StructuredIndexPartReader reader = StructuredIndex.openIndexPart(args[1]);
      KeyIterator iterator = reader.getIterator();
      while (!iterator.isDone()) {
        output.println(iterator.getKey());
        iterator.nextKey();
      }
      reader.close();
    }
  }

  private class DumpKeyValueFn implements AppFunction {

    public String getHelpString() {
      return "galago dump-keys <indexwriter-file> <key>\n\n"
              + "  Dumps all data associated with a particular key from a file\n"
              + "  created by IndexWriter.  This includes corpus files and all\n"
              + "  index files built by Galago.\n";
    }

    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 2) {
        output.println(getHelpString());
        return;
      }
      String key = args[2];
      output.printf("Dumping key: %s\n", key);
      StructuredIndexPartReader reader = StructuredIndex.openIndexPart(args[1]);
      KeyIterator iterator = reader.getIterator();

      if (iterator.skipToKey(Utility.fromString(key))) {
        ValueIterator vIter = iterator.getValueIterator();
        while (!vIter.isDone()) {
          output.printf("%s\n", vIter.getEntry());
          vIter.next();
        }
      }
    }
  }

  private class DumpModifierFn implements AppFunction {

    public String getHelpString() {
      return "galago dump-modifier <modifier file>\n\n"
              + "  Dumps the contents of the specified modifier file.\n";
    }

    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 1) {
        output.println(getHelpString());
        return;
      }

      StructuredIndexPartModifier modifier = StructuredIndex.openIndexModifier(args[1]);
      modifier.printContents(System.out);
      modifier.close();
    }
  }

  private class EvalFn implements AppFunction {

    public String getHelpString() {
      return "galago eval <args>: \n"
              + "   There are two ways to use this program.  First, you can evaluate a single ranking: \n"
              + "      galago eval TREC-Ranking-File TREC-Judgments-File\n"
              + "   or, you can use it to compare two rankings with statistical tests: \n"
              + "      galago eval TREC-Baseline-Ranking-File TREC-Improved-Ranking-File TREC-Judgments-File\n"
              + "   you can also include randomized tests (these take a bit longer): \n"
              + "      galago eval TREC-Baseline-Ranking-File TREC-Treatment-Ranking-File TREC-Judgments-File randomized\n\n"
              + "Single evaluation:\n"
              + "   The first column is the query number, or 'all' for a mean of the metric over all queries.\n"
              + "   The second column is the metric, which is one of:                                        \n"
              + "       num_ret        Number of retrieved documents                                         \n"
              + "       num_rel        Number of relevant documents listed in the judgments file             \n"
              + "       num_rel_ret    Number of relevant retrieved documents                                \n"
              + "       map            Mean average precision                                                \n"
              + "       bpref          Bpref (binary preference)                                             \n"
              + "       ndcg           Normalized Discounted Cumulative Gain, computed over all documents    \n"
              + "       ndcg15         Normalized Discounted Cumulative Gain, 15 document cutoff             \n"
              + "       Pn             Precision, n document cutoff                                          \n"
              + "       R-prec         R-Precision                                                           \n"
              + "       recip_rank     Reciprocal Rank (precision at first relevant document)                \n"
              + "   The third column is the metric value.                                                    \n\n"
              + "Compared evaluation: \n"
              + "   The first column is the metric (e.g. averagePrecision, ndcg, etc.)\n"
              + "   The second column is the test/formula used:                                               \n"
              + "       baseline       The baseline mean (mean of the metric over all baseline queries)       \n"
              + "       treatment      The \'improved\' mean (mean of the metric over all treatment queries)  \n"
              + "       basebetter     Number of queries where the baseline outperforms the treatment.        \n"
              + "       treatbetter    Number of queries where the treatment outperforms the baseline.        \n"
              + "       equal          Number of queries where the treatment and baseline perform identically.\n"
              + "       ttest          P-value of a paired t-test.\n"
              + "       signtest       P-value of the Fisher sign test.                                       \n"
              + "       randomized      P-value of a randomized test.                                          \n"
              + "   The second column also includes difference tests.  In these tests, the null hypothesis is \n"
              + "     that the mean of the treatment is at least k times the mean of the baseline.  We run the\n"
              + "     same tests as before, but we artificially improve the baseline values by a factor of k. \n"
              + "       h-ttest-0.05    Largest value of k such that the ttest has a p-value of less than 0.5. \n"
              + "       h-signtest-0.05 Largest value of k such that the sign test has a p-value of less than 0.5. \n"
              + "       h-randomized-0.05 Largest value of k such that the randomized test has a p-value of less than 0.5. \n"
              + "       h-ttest-0.01    Largest value of k such that the ttest has a p-value of less than 0.1. \n"
              + "       h-signtest-0.01 Largest value of k such that the sign test has a p-value of less than 0.1. \n"
              + "       h-randomized-0.01 Largest value of k such that the randomized test has a p-value of less than 0.1. \n"
              + "  The third column is the value of the test.\n";
    }

    public void run(String[] args, PrintStream output) throws Exception {
      org.galagosearch.core.eval.Main.main(Utility.subarray(args, 1));
    }
  }

  private class HelpFn implements AppFunction {

    public String getHelpString() {
      return "galago help [<function>]+\n\n"
              + "   Prints the usage information for any galago function.";
    }

    public void run(String[] args, PrintStream output) throws Exception {

      StringBuilder defaultOutput = new StringBuilder(
              "Type 'galago help <command>' to get more help about any command,\n"
              + "   or 'galago help all' to see all the documentation at once.\n\n"
              + "Popular commands:\n"
              + "   build-fast\n"
              + "   search\n"
              + "   batch-search\n\n"
              + "All commands:\n");
      List<String> cmds = new ArrayList(appFunctions.keySet());
      Collections.sort(cmds);
      for (String cmd : cmds) {
        defaultOutput.append("   ").append(cmd).append("\n");
      }

      // galago help 
      if (args.length == 0) {
        output.println(defaultOutput);
        output.println();
      } else if (args.length == 1) {
        output.println(getHelpString());
        output.println();
        output.println(defaultOutput);
        output.println();
      } else {
        for (String arg : Utility.subarray(args, 1)) {
          output.println("function: " + arg + "\n");
          output.println(appFunctions.get(arg).getHelpString());
          output.println();
        }
      }
    }
  }

  private class MakeCorpusFn implements AppFunction {

    public String getHelpString() {
      return "galago make-corpus [flags]+ --corpusPath=<corpus> (--inputPaths=<input>)+\n\n"
              + "  Copies documents from input files into a corpus file.  A corpus\n"
              + "  structure is required to use any of the document lookup features in \n"
              + "  Galago, like printing snippets of search results.\n\n"
              + "<corpus>: Corpus output path or directory\n\n"
              + "<input>:  Can be either a file or directory, and as many can be\n"
              + "          specified as you like.  Galago can read html, xml, txt, \n"
              + "          arc (Heritrix), trectext, trecweb and corpus files.\n"
              + "          Files may be gzip compressed (.gz).\n\n"
              + "Algorithm Flags:\n"
              + "  --corpusFormat={folder|file}: Selects which format of corpus to produce.\n"
              + "                           File is a single file corpus. Folder is a folder of data files with an index.\n"
              + "                           The folder structure can be produce in a parallel manner.\n"
              + "                           [default=folder]\n\n"
              + "Tupleflow Flags:\n"
              + "  --printJob={true|false}: Simply prints the execution plan of a Tupleflow-based job then exits.\n"
              + "                           [default=false]\n"
              + "  --mode={local|threaded|drmaa}: Selects which executor to use \n"
              + "                           [default=local]\n"
              + "  --port={int<65000} :     port number for web based progress monitoring. \n"
              + "                           [default=randomly selected free port]\n"
              + "  --galagoTemp=/path/to/temp/dir/: Sets the galago temp dir \n"
              + "                           [default = uses folders specified in ~/.galagotmp or java.io.tmpdir]\n"
              + "  --deleteOutput={0|1|2}:    Selects how much of the galago temp dir to delete\n"
              + "                           0 --> keep all data\n"
              + "                           1 --> delete all data + keep jobs directory (only useful for drmaa mode)\n"
              + "                           2 --> delete entire temp directory\n"
              + "                           [default=2]\n"
              + "  --distrib={int > 1}:     Selects the number of simultaneous jobs to create\n"
              + "                           [default = 10]\n";
    }

    public void run(String[] args, PrintStream output) throws Exception {
      Parameters p = new Parameters(args);
      p.set("command", Utility.join(args, " "));

      if (!p.containsKey("corpusPath")
              || (p.list("corpusPath").size() > 1)
              || !p.containsKey("inputPaths")) {
        output.println(getHelpString());
        return;
      }

      MakeCorpus mc = new MakeCorpus();
      Job job = mc.getMakeCorpusJob(p);

      runTupleFlowJob(job, p, output);
    }
  }

  private class MergeIndexFn implements AppFunction {

    public String getHelpString() {
      return "galago merge-index [<flags>+] --indexPath=<output> (--inputPaths=<input>)+\n\n"
              + "  Merges 2 or more indexes. Assumes that the document numberings\n"
              + "  are non-unique. So all documents are assigned new internal numbers.\n\n"
              + "<output>:  Directory to be created that contains the merged index\n\n"
              + "<input>:  Directory containing an index to be merged \n\n"
              + "Algorithm Flags:\n\n"
              + "Tupleflow Flags:\n"
              + "  --printJob={none|plan|dot}: Simply prints the execution plan of a Tupleflow-based job then exits.\n"
              + "                           [default=none]\n"
              + "  --mode={local|threaded|drmaa}: Selects which executor to use \n"
              + "                           [default=local]\n"
              + "  --port={int<65000} :     port number for web based progress monitoring. \n"
              + "                           [default=randomly selected free port]\n"
              + "  --galagoTemp=/path/to/temp/dir/: Sets the galago temp dir \n"
              + "                           [default = uses folders specified in ~/.galagotmp or java.io.tmpdir]\n"
              + "  --deleteOutput={0|1|2}:    Selects how much of the galago temp dir to delete\n"
              + "                           0 --> keep all data\n"
              + "                           1 --> delete all data + keep jobs directory (only useful for drmaa mode)\n"
              + "                           2 --> delete entire temp directory\n"
              + "                           [default=0]\n"
              + "  --distrib={int > 1}:     Selects the number of simultaneous jobs to create\n"
              + "                           [default = 10]\n";
    }

    public void run(String[] args, PrintStream output) throws Exception {
      Parameters p = new Parameters(args);
      p.set("command", Utility.join(args, " "));

      if (!p.containsKey("indexPath")
              || (p.list("indexPath").size() > 1)
              || !p.containsKey("inputPaths")) {
        output.println(getHelpString());
        return;
      }

      Job job;
      MergeIndexes build = new MergeIndexes();
      job = build.getJob(p);

      App2.runTupleFlowJob(job, p, output);
    }
  }

  private class SearchFn implements AppFunction {

    public String getHelpString() {
      return "galago search <args> \n\n"
              + "  Starts a web interface for searching an index interactively.\n"
              + "  The URL to use in your web browser will appear in the command \n"
              + "  output.  Cancel the process (Control-C) to quit.\n\n"
              + "  If you specify a parameters file, you can direct Galago to load \n"
              + "  extra operators or traversals from your own jar files.  See \n"
              + "  the documentation in \n"
              + "  org.galagosearch.core.retrieval.structured.FeatureFactory for more\n"
              + "  information.\n\n"
              + "  Parameters availiable:\n"
              + "   --corpus={file path} : corpus file path\n"
              + "   --index={file path}  : index file path\n"
              + "   --index={url}        : galago search url (for distributed retrieval)\n"
              + "   --port={int<65000}   : port number for web retrieval.\n\n"
              + "  Parameters can also be input through a configuration file.\n"
              + "  For example: search.parameters\n"
              + "  <parameters>\n"
              + "   <index>/path/to/index1</index>\n"
              + "   <index>/path/to/index2</index>\n"
              + "   <corpus>/path/to/corpus</corpus>\n"
              + "  </parameters>\n\n"
              + "  Note that the set of  parameters must include at least one index path.\n";
    }

    public void run(String[] args, PrintStream output) throws Exception {
      Parameters p = new Parameters(Utility.subarray(args, 1));

      if (!p.containsKey("index")) {
        output.println(getHelpString());
        if (args.length > 1) { // special notice.
          output.println("-- NOTE: An index path is REQUIRED! --");
        }
        return;
      }

      Search search = new Search(p);
      int port = (int) p.get("port", 0);
      if (port == 0) {
        port = Utility.getFreePort();
      } else {
        if (!Utility.isFreePort(port)) {
          throw new IOException("Tried to bind to port " + port + " which is in use.");
        }
      }
      Server server = new Server(port);
      URLMappingHandler mh = new URLMappingHandler();
      mh.setHandler("/stream", new StreamContextHandler(search));
      mh.setHandler("/xml", new XMLContextHandler(search));
      mh.setHandler("/json", new JSONContextHandler(search));
      mh.setDefault(new SearchWebHandler(search));
      server.addHandler(mh);
      server.start();
      output.println("Server: http://localhost:" + port);

      // Ensure we print out the ip addr url as well
      InetAddress address = InetAddress.getLocalHost();
      String masterURL = String.format("http://%s:%d", address.getHostAddress(), port);
      output.println("ServerIP: " + masterURL);
    }
  }

  private class XCountFn implements AppFunction {

    public String getHelpString() {
      return "galago xcount --x=<countable-query> --index=<index> \n\n"
              + "  Returns the number of times the countable-query occurs.\n"
              + "  More than one index and expression can be specified.\n"
              + "  Examples of countable-expressions: terms, ordered windows and unordered windows.\n";
    }

    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 1) {
        output.println(getHelpString());
        return;
      }

      BatchSearch.xCount(args, output);
    }
  }

  private class XDocCountFn implements AppFunction {

    public String getHelpString() {
      return "galago doccount --x=<countable-query> --index=<index> \n\n"
              + "  Returns the number of documents that contain the countable-query.\n"
              + "  More than one index and expression can be specified.\n"
              + "  Examples of countable-expressions: terms, ordered windows and unordered windows.\n";
    }

    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 1) {
        output.println(getHelpString());
        return;
      }

      BatchSearch.docCount(args, output);
    }
  }

  // Static helper functions
  public static void runTupleFlowJob(Job job, Parameters p, PrintStream output) throws Exception {
    String printJob = p.get("printJob", "none");
    if (printJob.equals("plan")) {
      output.println(job.toString());
      return;
    } else if (printJob.equals("dot")) {
      output.println(job.toDotString());
      return;
    }

    int hash = (int) p.get("distrib", 0);
    if (hash > 0) {
      job.properties.put("hashCount", Integer.toString(hash));
    }

    ErrorStore store = new ErrorStore();
    JobExecutor.runLocally(job, store, p);
    if (store.hasStatements()) {
      output.println(store.toString());
    }
  }
}
