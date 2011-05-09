/*
 *  BSD License (http://www.galagosearch.org/license)
 */

package org.galagosearch.core.index.merge;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.types.DocumentMappingData;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ConnectionAssignmentType;
import org.galagosearch.tupleflow.execution.ErrorStore;
import org.galagosearch.tupleflow.execution.InputStep;
import org.galagosearch.tupleflow.execution.Job;
import org.galagosearch.tupleflow.execution.JobExecutor;
import org.galagosearch.tupleflow.execution.OutputStep;
import org.galagosearch.tupleflow.execution.Stage;
import org.galagosearch.tupleflow.execution.Step;

/**
 *
 * @author sjh
 */
public class MergeIndexes {

  private static PrintStream output;
  private String outputPath;
  private List<String> inputPaths;

  // tupleflow stage functions

  private Stage getNumberIndexStage(){
    Stage stage = new Stage("indexNumberer");

    stage.addOutput("indexes", new DocumentSplit.FileIdOrder());

    Parameters p = new Parameters();
    for(String inputPath : inputPaths){
      p.add("inputPath", inputPath);
    }
    stage.add(new Step(IndexNumberer.class, p));
    stage.add(new OutputStep("indexes"));
    return stage;
  }

  private Stage getDocumentMappingStage(){
    Stage stage = new Stage("documentMapper");

    stage.addInput("indexes", new DocumentSplit.FileIdOrder());
    stage.addOutput("documentMappingData", new DocumentMappingData.IndexIdOrder());

    stage.add( new InputStep("indexes"));
    stage.add( new Step(DocumentNumberMapper.class));
    stage.add( new OutputStep("documentMappingData"));

    return stage;
  }

  private Stage getPartMerger(String stageName, String part, String outputFile){
    Stage stage = new Stage(stageName);

    stage.addInput("indexes", new DocumentSplit.FileIdOrder());
    stage.addInput("documentMappingData", new DocumentMappingData.IndexIdOrder());

    stage.add(new InputStep("indexes"));
    Parameters p = new Parameters();
    p.add("mappingDataStream", "documentMappingData");
    p.add("part", part);
    p.add("filename", outputFile);
    stage.add(new Step(IndexPartMergeManager.class, p));

    return stage;
  }
  

  public Job getJob(Parameters p) throws IOException{
    Job job = new Job();

    this.outputPath = p.get("indexPath");
    this.inputPaths = new ArrayList();
    for(String input : p.stringList("inputPaths")){
      inputPaths.add((new File(input)).getAbsolutePath());
    }

    // get a list of shared mergable parts - by set intersection.
    HashSet<String> sharedParts = null;
    for( String index : p.stringList("inputPaths")){
      StructuredIndex i = new StructuredIndex(index);
      Set<String> partNames = i.getPartNames();
      HashSet<String> mergableParts = new HashSet();
      for(String part : partNames){
        if( i.openLocalIndexPart(part).getManifest().containsKey("mergerClass") ){
          mergableParts.add( part );
          // System.err.println( part + "\t" + i.openLocalIndexPart(part).getManifest().get("mergerClass") );
        }
      }

      if(sharedParts == null){
        sharedParts = new HashSet();
        sharedParts.addAll(mergableParts);
      }
      sharedParts.retainAll( mergableParts );
      i.close();
    }

    // log the parts to be merged.
    for(String part : sharedParts){
      Logger.getLogger(getClass().getName()).log(Level.INFO, "Merging Part: " + part);
    }

    job.add( getNumberIndexStage( ) );
    job.add( getDocumentMappingStage() );

    job.connect( "indexNumberer", "documentMapper", ConnectionAssignmentType.Combined );

    for(String part : sharedParts){
      job.add( getPartMerger(part+"MergeStage", part, outputPath + File.separator + part) );
      job.connect("indexNumberer", part+"MergeStage", ConnectionAssignmentType.Combined);
      job.connect("documentMapper", part+"MergeStage", ConnectionAssignmentType.Combined);
    }
    
    return job;
  }


  
  // static main functions

  public static void commandHelpMerge() {
      output.println("galago merge-index [<flags>+] <output> (<input>)+");
      output.println();
      output.println("  Merges 2 or more indexes. Assumes that the document numberings");
      output.println("  are non-unique. So all documents are assigned new internal numbers.");
      output.println();
      output.println("<output>:  Directory to be created that contains the merged index");
      output.println();
      output.println("<input>:  Directory containing an index to be merged ");
      output.println();
      output.println("Algorithm Flags:");
      output.println();
      output.println("Tupleflow Flags:");
      output.println("  --printJob={none|plan|dot}: Simply prints the execution plan of a Tupleflow-based job then exits.");
      output.println("                           [default=none]");
      output.println("  --mode={local|threaded|drmaa}: Selects which executor to use ");
      output.println("                           [default=local]");
      output.println("  --port={int<65000} :     port number for web based progress monitoring. ");
      output.println("                           [default=randomly selected free port]");
      output.println("  --galagoTemp=/path/to/temp/dir/: Sets the galago temp dir ");
      output.println("                           [default = uses folders specified in ~/.galagotmp or java.io.tmpdir]");
      output.println("  --deleteOutput={0|1|2}:    Selects how much of the galago temp dir to delete");
      output.println("                           0 --> keep all data");
      output.println("                           1 --> delete all data + keep jobs directory (only useful for drmaa mode)");
      output.println("                           2 --> delete entire temp directory");
      output.println("                           [default=0]");
      output.println("  --distrib={int > 1}:     Selects the number of simultaneous jobs to create");
      output.println("                           [default = 10]");
 }


  public static void main(String[] args) throws IOException, Exception{
    output = System.out;
    if (args.length < 3) { // build index input
      commandHelpMerge();
      return;
    }

    String[][] filtered = Utility.filterFlags(args);

    String[] flags = filtered[0];
    String[] nonFlags = filtered[1];
    String indexName = nonFlags[1];
    String[] docs = Utility.subarray(nonFlags, 2);

    Parameters p = new Parameters(flags);
    p.set("command", Utility.join(args, " "));
    p.add("indexPath", indexName);
    for (String doc : docs) {
      p.add("inputPaths", doc);
    }

    Job job;
    MergeIndexes build = new MergeIndexes();
    job = build.getJob(p);

    String printJob = p.get("printJob", "none");
    if (printJob.equals("plan")) {
      System.out.println(job.toString());
      return;
    } else if (printJob.equals("dot")) {
      System.out.println(job.toDotString());
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
