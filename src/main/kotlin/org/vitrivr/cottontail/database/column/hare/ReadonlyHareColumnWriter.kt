package org.vitrivr.cottontail.database.column.hare

import org.vitrivr.cottontail.model.basics.TransactionId
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.exceptions.TransactionException
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnWriter

/**
 * This is a mock implementation of [HareColumnWriter] that throws
 * [TransactionException.TransactionReadOnlyException] for all attempts to access a specific method.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class ReadonlyHareColumnWriter<T : Value>(override val tid: TransactionId) : HareColumnWriter<T> {
    override val isOpen: Boolean
        get() = false

    override fun update(tupleId: TupleId, value: T?) {
        throw IllegalStateException("Transaction $tid is read-only.")
    }

    override fun compareAndUpdate(tupleId: TupleId, expectedValue: T?, newValue: T?): Boolean {
        throw IllegalStateException("Transaction $tid is read-only.")
    }

    override fun delete(tupleId: TupleId): T? {
        throw IllegalStateException("Transaction $tid is read-only.")
    }

    override fun append(value: T?): TupleId {
        throw IllegalStateException("Transaction $tid is read-only.")
    }

    override fun commit() {
        throw IllegalStateException("Transaction $tid is read-only.")
    }

    override fun rollback() {
        throw IllegalStateException("Transaction $tid is read-only.")
    }

    override fun close() {
        /* No op. */
    }
}