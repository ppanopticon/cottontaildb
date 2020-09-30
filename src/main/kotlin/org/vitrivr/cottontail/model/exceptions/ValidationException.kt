package org.vitrivr.cottontail.model.exceptions

import org.vitrivr.cottontail.model.basics.Name


/**
 * A class of [DatabaseException]s that are thrown whenever data validation fails usually during inserts and updates.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
open class ValidationException(message: String) : DatabaseException(message) {
    /**
     * Thrown by [Index][org.vitrivr.cottontail.database.index.Index] structures whenever their rebuild fails because of data constraints.
     *
     * @param index The FQN of the index that was affected.
     * @param message A message describing the problem.
     */
    class IndexUpdateException(index: Name, message: String) : ValidationException("Index '$index' rebuild failed due to an error: $message")
}