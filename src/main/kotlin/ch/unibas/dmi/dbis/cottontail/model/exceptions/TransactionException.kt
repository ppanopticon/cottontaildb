package ch.unibas.dmi.dbis.cottontail.model.exceptions

import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import org.mapdb.DBException
import java.util.*


/**
 * These exceptions are thrown whenever a [Transaction][ch.unibas.dmi.dbis.cottontail.database.general.Transaction]
 * or an action making up a [Transaction][ch.unibas.dmi.dbis.cottontail.database.general.Transaction] fails for some reason.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
open class TransactionException(message: String) : DatabaseException(message) {
    /** [Transaction][ch.unibas.dmi.dbis.cottontail.database.general.Transaction]
     * could not be created because enclosing DBO was closed.
     *
     * @param tid The ID of the [Tx] in which this error occurred.
     */
    class TransactionDBOClosedException(tid: UUID): TransactionException("The enclosing DBO has been closed. Transaction $tid could not be created!")

    /**
     * [Transaction][ch.unibas.dmi.dbis.cottontail.database.general.Transaction]
     * cannot be used anymore, because it is in error.
     *
     * @param tid The ID of the [Tx] in which this error occurred.
     */
    class TransactionClosedException(tid: UUID): TransactionException("Transaction $tid has been closed and cannot be used anymore.")

    /**
     * [Transaction][ch.unibas.dmi.dbis.cottontail.database.general.Transaction]
     * cannot be used anymore, because it was closed already.
     *
     * @param tid The ID of the [Tx] in which this error occurred.
     */
    class TransactionInErrorException(tid: UUID): TransactionException("Transaction $tid is in error and cannot be used, until it is rolled back.")

    /**
     * Write could not be executed, because [Transaction][ch.unibas.dmi.dbis.cottontail.database.general.Transaction]
     * is read-only.
     *
     * @param tid The ID of the [Transaction][ch.unibas.dmi.dbis.cottontail.database.general.Transaction] in which this error occurred.
     */
    class TransactionReadOnlyException(tid: UUID): TransactionException("Transaction $tid is read-only and cannot be used to alter data.")

    /**
     * Write could not be executed, because [Transaction][ch.unibas.dmi.dbis.cottontail.database.general.Transaction]
     * was unable to acquire the necessary locks (usually on DBOs).
     *
     * @param tid The ID of the [Transaction][ch.unibas.dmi.dbis.cottontail.database.general.Transaction] in which this error occurred.
     */
    class TransactionWriteLockException(tid: UUID): TransactionException("Transaction $tid was unable to obtain the necessary locks.")

    /**
     * Write could not be executed because it failed a validation step. This is usually caused by a user error, providing wrong data.
     *
     * @param tid The ID of the [Transaction][ch.unibas.dmi.dbis.cottontail.database.general.Transaction] in which this error occurred.
     * @param message Description of the validation error.
     */
    class TransactionValidationException(tid: UUID, message: String): TransactionException("Transaction $tid reported validation error: $message")

    /**
     * Read/write could not be executed because it caused an error in the underlying data store. This is usually a critical condition and
     * can be caused by either system failure, external manipulation or serious bugs.
     *
     * @param tid The ID of the [Transaction][ch.unibas.dmi.dbis.cottontail.database.general.Transaction] in which this error occurred.
     * @param message Description of the storage error.
     */
    class TransactionStorageException(tid: UUID, message: String): TransactionException("Transaction $tid reported storage error: $message")

    /**
     * Read/write could not be executed because some of the tuple IDs was invalid
     *
     * @param tid The ID of the [Transaction][ch.unibas.dmi.dbis.cottontail.database.general.Transaction] in which this error occurred.
     * @param tupleId The tupleId that was wrong.
     */
    class InvalidTupleId(tid: UUID, tupleId: Long): DBException("Transaction $tid reported an error: The provided tuple ID $tupleId is out of bounds and therefore invalid.")

    /**
     * Read/write could not be executed because some of the colums don't exist.
     *
     * @param tid The ID of the [Transaction][ch.unibas.dmi.dbis.cottontail.database.general.Transaction] in which this error occurred.
     * @param column The definition of the [Column][ch.unibas.dmi.dbis.cottontail.database.column.Column] that is missing.
     */
    class ColumnUnknownException(tid: UUID, column: ColumnDef<*>): TransactionException("Transaction $tid could not be executed, because column '${column.name}' (type=${column.type.name}, size=${column.size}) does not either not exist or has a different type.")
}