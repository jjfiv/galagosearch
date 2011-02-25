/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.Utility;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.Session;
import org.ggf.drmaa.SessionFactory;

/**
 *
 * @author sjh
 */
public class IndexShardControl {

    public static void printUsage(PrintStream output){
        output.println("galago start-parallel <outputParameterPath> <index-parameters>");
        output.println();
        output.println("    Starts and stops a set of distributed retrieval jobs");
        output.println("    <index-parameters> is the set of parameters that you");
        output.println("        could use with the search or batch-search commands.");
        output.println("    <outputParameterPath> is path of the output parameter file.");
        output.println("        The output parameters are nearly identical to the input");
        output.println("        parameters. Index paths are changed and jobIds are added.");
        output.println();
        output.println("galago stop-parallel <parameterFile>");
        output.println();
        output.println("    Stops the set of jobIdspresent in the parameter file.");
        output.println("    Sends a kill command for each jobId.");
        output.println();
    }

    /**
     * Idea here is to start one job
     * for each index shard within some parallel environment
     *  (eg: DRMAA)
     */
    public static void start(String[] args, PrintStream output) throws Exception {
        if(args.length <= 2){
            printUsage(output);
        }

        File outputFile = new File(args[1]);
        assert outputFile.getParentFile().isDirectory() : outputFile.getParent() + " does not exist.";

        Parameters parameters = new Parameters(Utility.subarray(args, 2));
        Parameters newParameters;
        // use the drmaa submission interface
        if (parameters.get("drmaa", true)) {
            newParameters = DRMAASubmit(parameters);
        } else {
            newParameters = parameters;
        }

        Utility.copyStringToFile(newParameters.toString(), outputFile);
    }

    public static void stop(String[] args, PrintStream output) throws Exception {
        if(args.length <= 1){
            printUsage(output);
        }

        Parameters parameters = new Parameters(Utility.subarray(args, 1));
        if (parameters.get("drmaa", true)) {
            DRMAAKill(parameters);
        }


    }

    private static Parameters DRMAASubmit(Parameters parameters) throws DrmaaException, IOException, InterruptedException {
        Parameters newParameters = parameters.clone();
        newParameters.set("index", ""); // empty index list
        newParameters.set("corpus", ""); // empty corpus list
        newParameters.set("port", ""); // empty port list

        int port = (int) parameters.get("port", Utility.getFreePort());

        // first extract the set of indexes
        HashMap<Integer, String> paths = new HashMap();
        HashMap<Integer, String> idStrings = new HashMap();
        int indexId = 0;
        for (Value value : parameters.list("index")) {

            String path;
            String id = "all";
            if (value.containsKey("path")) {
                path = value.get("path").toString();
                if (value.containsKey("id")) {
                    id = value.get("id").toString();
                }
            } else {
                path = value.toString();
            }
            paths.put(indexId, path);
            idStrings.put(indexId, id);
            indexId++;
        }

        // check that we have 'some' index to submit
        assert (paths.size() > 0) : "Could not find an index to submit";

        // create a temp directory
        File tempFolder = Utility.createGalagoTempDir();

        // copy the new parameters to a file
        // this will ensure any special user settings are propagated to shard nodes
        File pTemp = new File(tempFolder.getAbsolutePath() + File.separator + "p");
        Utility.copyStringToFile(newParameters.toString(), pTemp);

        // Initialise and prepare drmaa session
        Session session = SessionFactory.getFactory().getSession();
        session.init("");

        String command = System.getenv("JAVA_HOME") + File.separator + "bin/java";
        String[] arguments = new String[10];
        arguments[0] = "-ea";
        arguments[1] = "-Xmx1700m";
        arguments[2] = "-Xms1700m";
        arguments[3] = "-cp";
        arguments[4] = System.getProperty("java.class.path");
        arguments[5] = "org.galagosearch.core.tools.App";
        arguments[6] = "search";
        arguments[7] = pTemp.getAbsolutePath();

        LinkedList<String> queuedJobs = new LinkedList();
        for (int i : paths.keySet()) {

            // swap in this particular index shard
            arguments[8] = "--index=" + paths.get(i);
            int shardPort = 1 + i + port;
            arguments[9] = "--port=" + shardPort;

            // Create the fill a DRMAA job template.
            JobTemplate template = session.createJobTemplate();
            template.setJobName("galago-indexShard-" + i);
            template.setRemoteCommand(command);
            template.setArgs(arguments);
            template.setOutputPath(":" + tempFolder.getAbsolutePath()
                    + File.separator
                    + "stdout." + i);
            template.setErrorPath(":" + tempFolder.getAbsolutePath()
                    + File.separator
                    + "stderr." + i);

            // Run the job.
            String jobId = session.runJob(template);

            // keep track of count and job id
            newParameters.add("jobId", jobId);

            queuedJobs.add(jobId);
        }

        // We now need to wait for each job to start
        while (queuedJobs.size() > 0) {
            String jobId = queuedJobs.poll();
            int status = session.getJobProgramStatus(jobId);
            if (status == session.RUNNING) {
                // do nothing - jobId already removed
                // continue with next job
            } else if ((status == session.DONE)
                    || (status == session.FAILED)) {
                // failure - something's gone wrong.
                throw new IOException("JOB " + status + ": has failed or quit.");
            } else {
                // job not running - return it to the queue
                queuedJobs.offer(jobId);
                // and sleep for 2 second (give other jobs a chance to start)
                Thread.sleep(2000);
            }
        }

        // now we have all of the session data we need
        session.exit();


        // now collect the http addresses from the stdout
        for (int i : paths.keySet()) {
            // open stdout
            File stdout = new File(tempFolder.getAbsolutePath()
                    + File.separator
                    + "stdout." + i);
            BufferedReader reader = new BufferedReader(new FileReader(stdout));
            // find ip:port line
            String line;
            String url = null;

            while ((line = reader.readLine()) != null) {
                if (line.startsWith("IPStatus")) {
                    url = line.split(" ")[1];
                    break;
                }
            }
            
            if(url == null){
                throw new IOException("Can't find the submitted url for index path:" + paths.get(i));
            }

            // add this addr to newParameters
            Value index = new Value();
            index.add("path", url);
            index.add("id", idStrings.get(i));

            newParameters.add("index", Collections.singletonList(index));
        }

        newParameters.add("port", Integer.toString(port));
        if(parameters.containsKey("corpus")){
            newParameters.add("corpus", parameters.list("corpus"));
        }

        // new parameters now has a port, corpus paths and a set of index urls + ids
        return newParameters;
    }

    private static void DRMAAKill(Parameters parameters) throws DrmaaException {
        // Initialise and prepare drmaa session
        Session session = SessionFactory.getFactory().getSession();
        session.init("");

        for(String jobId : parameters.stringList("jobId")){
            session.control(jobId, Session.TERMINATE);
        }
        session.exit();
    }
}
