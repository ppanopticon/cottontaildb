package org.vitrivr.cottontail.model.exceptions

import org.vitrivr.cottontail.model.basics.TransactionId


/**
 * These [Exception]s are thrown whenever a [org.vitrivr.cottontail.database.general.Tx] or an action
 * making up a [org.vitrivr.cottontail.database.general.Tx] fails for some reason.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
open class TxException(message: String) : DatabaseException(message) {
    /** [org.vitrivr.cottontail.database.general.Tx] could not be created because enclosing DBO was closed.
     *
     * @param tid The [TransactionId] of the [org.vitrivr.cottontail.database.general.Tx] in which this error occurred.
     */
    class TxDBOClosedException(tid: TransactionId) : TxException("Tx $tid could not be created: Enclosing DBO has been closed.")

    /**
     * [org.vitrivr.cottontail.database.general.Tx] cannot be used anymore, because it was closed already.
     *
     * @param tid The [TransactionId] of the [org.vitrivr.cottontail.database.general.Tx] in which this error occurred.
     */
    class TxClosedException(tid: TransactionId) : TxException("Tx $tid has been closed and cannot be used anymore.")

    /**
     * [org.vitrivr.cottontail.database.general.Tx] cannot be used anymore, because it is in error.
     *
     * @param tid The [TransactionId] of the [org.vitrivr.cottontail.database.general.Tx] in which this error occurred.
     */
    class TxInErrorException(tid: TransactionId) : TxException("Tx $tid is in error and cannot be used, until it is rolled back.")

    /**
     * Write could not be executed because it failed a validation step. This is often caused by a user error, providing erroneous data.
     *
     * @param tid The [TransactionId] of the [org.vitrivr.cottontail.database.general.Tx] in which this error occurred.
     * @param message Description of the validation error.
     */
    class TxValidationException(tid: TransactionId, message: String) : TxException("Transaction $tid reported validation error: $message")

    /**
     * Read/write could not be executed because it caused an error in the underlying data store. This is usually a critical condition and
     * can be caused by either system failure, external manipulation or serious bugs.
     *
     * @param tid The [TransactionId] of the [org.vitrivr.cottontail.database.general.Tx] in which this error occurred.
     * @param message Description of the storage error.
     */
    class TxStorageException(tid: TransactionId, message: String) : TxException("Transaction $tid reported storage error: $message")
}