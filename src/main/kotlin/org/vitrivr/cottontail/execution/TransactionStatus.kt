package org.vitrivr.cottontail.execution

/**
 * Status of the [TransactionManager.Transaction].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
enum class TransactionStatus {
    READY,

    /** [TransactionManager.Transaction] is ready and able to process queries. */
    RUNNING,

    /** [TransactionManager.Transaction] is running and therefore new queries have to wait. */
    ERROR,

    /** [TransactionManager.Transaction] was marked for rollback due to an execution error. */
    ROLLBACK,

    /** [TransactionManager.Transaction] was rolled back. Execution not possible. */
    COMMIT,
    /** [TransactionManager.Transaction] was committed. Execution not possible. */
}