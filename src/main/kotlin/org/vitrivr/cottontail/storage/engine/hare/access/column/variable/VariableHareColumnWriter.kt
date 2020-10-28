package org.vitrivr.cottontail.storage.engine.hare.access.column.variable

import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnWriter
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer

/**
 * A [HareColumnWriter] implementation for [VariableHareColumnFile]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class VariableHareColumnWriter<T: Value> (val file: VariableHareColumnFile<T>): HareColumnWriter<T> {
    /** [BufferPool] for this [FixedHareColumnWriter] is always the one used by the [FixedHareColumnFile] (core pool). */
    private val bufferPool = this.file.bufferPool

    /** The [Serializer] used to read data through this [FixedHareColumnReader]. */
    private val serializer: Serializer<T> = this.file.columnDef.serializer

    override fun update(tupleId: TupleId, value: T?) {
        TODO("Not yet implemented")
    }

    override fun compareAndUpdate(tupleId: TupleId, expectedValue: T?, newValue: T?): Boolean {
        TODO("Not yet implemented")
    }

    override fun delete(tupleId: TupleId): T? {
        TODO("Not yet implemented")
    }

    override fun append(value: T?): TupleId {
        TODO("Not yet implemented")
    }
}