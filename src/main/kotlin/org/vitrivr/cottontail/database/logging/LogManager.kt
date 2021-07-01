package org.vitrivr.cottontail.database.logging

import org.vitrivr.cottontail.database.logging.operations.Operation
import org.vitrivr.cottontail.model.basics.TransactionId
import java.util.*

class LogManager {
    /** List of ongoing [TransactionId]s. */
    val ongoing: List<TransactionId> = LinkedList<TransactionId>()

    /**
     * Logs an [Operation] through this [LogManager] and returns a sequence number for the entry.
     */
    fun log(operation: Operation): Long {
        TODO()
    }

    /**
     * Tries to truncate this log by
     */
    fun truncate(): Boolean {
        TODO()
    }
}