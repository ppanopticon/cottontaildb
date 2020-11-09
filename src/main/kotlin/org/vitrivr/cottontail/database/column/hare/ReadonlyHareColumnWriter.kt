package org.vitrivr.cottontail.database.column.hare

import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.exceptions.TransactionException
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnWriter
import java.util.*

/**
 * This is a mock implementation of [HareColumnWriter] that throws
 * [TransactionException.TransactionReadOnlyException] for all attempts to access a specific method.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class ReadonlyHareColumnWriter<T : Value>(val tid: UUID) : HareColumnWriter<T> {
    override val isOpen: Boolean
        get() = false

    override fun update(tupleId: TupleId, value: T?) {
        throw TransactionException.TransactionReadOnlyException(this.tid)
    }

    override fun compareAndUpdate(tupleId: TupleId, expectedValue: T?, newValue: T?): Boolean {
        throw TransactionException.TransactionReadOnlyException(this.tid)
    }

    override fun delete(tupleId: TupleId): T? {
        throw TransactionException.TransactionReadOnlyException(this.tid)
    }

    override fun append(value: T?): TupleId {
        throw TransactionException.TransactionReadOnlyException(this.tid)
    }

    override fun commit() {
        throw TransactionException.TransactionReadOnlyException(this.tid)
    }

    override fun rollback() {
        throw TransactionException.TransactionReadOnlyException(this.tid)
    }

    override fun close() {
        throw TransactionException.TransactionReadOnlyException(this.tid)
    }
}