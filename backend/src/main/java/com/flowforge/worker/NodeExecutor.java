package com.flowforge.worker;

import com.flowforge.model.TaskMessage;

/**
 * Interface implemented by all pipeline node executors.
 */
public interface NodeExecutor {
    /**
     * Executes the node task and returns the number of records processed.
     * Throws an exception on failure.
     */
    long execute(TaskMessage task) throws Exception;
}
