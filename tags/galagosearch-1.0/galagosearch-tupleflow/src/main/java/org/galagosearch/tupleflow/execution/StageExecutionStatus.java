// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.tupleflow.execution;

import java.util.List;

/**
 *
 * @author trevor
 */
public interface StageExecutionStatus {
    public String getName();
    public int getBlockedInstances();
    public int getQueuedInstances();
    public int getRunningInstances();
    public int getCompletedInstances();

    public boolean isDone();
    public List<Exception> getExceptions();
}
