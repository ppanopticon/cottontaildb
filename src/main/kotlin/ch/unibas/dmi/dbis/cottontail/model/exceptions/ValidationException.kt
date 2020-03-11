package ch.unibas.dmi.dbis.cottontail.model.exceptions

import ch.unibas.dmi.dbis.cottontail.utilities.name.Name


/**
 * A class of [DatabaseException]s that are thrown whenever data validation fails usually during inserts and updates.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
open class ValidationException(message: String) : DatabaseException(message) {
    /**
     * Thrown by [Index][ch.unibas.dmi.dbis.cottontail.database.index.Index] structures whenever their rebuild fails because of data constraints.
     *
     * @param index The FQN of the index that was affected.
     * @param message A message describing the problem.
     */
    class IndexUpdateException(index: Name, message: String): ValidationException("Index '$index' rebuild failed due to an error: $message")
}