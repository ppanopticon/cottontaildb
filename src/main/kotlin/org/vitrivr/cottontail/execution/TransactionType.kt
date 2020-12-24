package org.vitrivr.cottontail.execution

/**
 * Type of [TransactionManager.Transaction].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class TransactionType {
    USER, /* A [Transaction] explicitly created by a user. */
    USER_IMPLICIT, /* A [Transaction] implicitly created by a user (i.e. by issuing a query). */
    SYSTEM /* A [Transaction] implicitly created by the system. */
}