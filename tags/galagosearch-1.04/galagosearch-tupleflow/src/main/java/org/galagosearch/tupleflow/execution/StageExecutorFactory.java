// BSD License (http://www.galagosearch.org/license)
 
package org.galagosearch.tupleflow.execution;

import org.galagosearch.tupleflow.Utility;
import java.util.Arrays;

/**
 *
 * @author trevor
 */
public class StageExecutorFactory {
    public static StageExecutor newInstance(String name, String... args) {
        if (name == null) {
            name = "local";
        }
        name = name.toLowerCase();

        if (name.startsWith("class=")) {
            String[] fields = name.split("=");
            assert fields.length >= 2;
            String className = fields[1];

            try {
                Class actual = Class.forName(className);
                return (StageExecutor) actual.newInstance();
            } catch (Exception e) {
                return null;
            }
        } else if (name.startsWith("thread") || name.startsWith("local")) {
            return new ThreadedStageExecutor();
        } else if (name.startsWith("ssh")) {
            return new SSHStageExecutor(args[0], Arrays.asList(Utility.subarray(args, 1)));
        } else if (name.equals("remotedebug")) {
            return new LocalRemoteStageExecutor();
        } else {
            return new LocalStageExecutor();
        }
    }
}
