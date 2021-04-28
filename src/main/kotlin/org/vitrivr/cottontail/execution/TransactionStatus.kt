package org.vitrivr.cottontail.execution

/**
 * Status of the [TransactionManager.Transaction].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
enum class TransactionStatus {
    /** [TransactionManager.Transaction] is ready and able to process queries. */
    READY,

    /** [TransactionManager.Transaction] is running and therefore new queries have to wait. */
    RUNNING,

    /** [TransactionManager.Transaction] is committing or rolling back and therefore new queries have to wait. */
    ERROR,

    /** [TransactionManager.Transaction] was marked for rollback due to an execution error. */
    FINALIZING,

    /** [TransactionManager.Transaction] was rolled back. Execution not possible. */
    ROLLBACK,

    /** [TransactionManager.Transaction] was committed. Execution not possible. */
    COMMIT,
}