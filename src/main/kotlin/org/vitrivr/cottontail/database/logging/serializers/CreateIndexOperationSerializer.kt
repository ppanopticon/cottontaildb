package org.vitrivr.cottontail.database.logging.serializers

import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.logging.operations.Operation
import org.vitrivr.cottontail.database.logging.operations.OperationType
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.utilities.extensions.getString
import org.vitrivr.cottontail.utilities.extensions.putString
import java.nio.ByteBuffer

object CreateIndexOperationSerializer: Serializer<Operation.CreateIndexOperation>() {
    override fun sizeOf(operation: Operation.CreateIndexOperation): Int = 12 +
    (4 + operation.index.components[1].length * Char.SIZE_BYTES) +
    (4 + operation.index.components[2].length * Char.SIZE_BYTES) +
    (4 + operation.index.components[3].length * Char.SIZE_BYTES) +
    4 + operation.columns.sumOf { 4 + (it.simple.length * Char.SIZE_BYTES) } +
    4 + operation.params.entries.sumOf { 4 + it.key.length * Char.SIZE_BYTES + 3 + it.value.length * Char.SIZE_BYTES }

    override fun serialize(operation: Operation.CreateIndexOperation): ByteBuffer {
        val buffer = ByteBuffer.allocate(this.sizeOf(operation))
        buffer.putInt(operation.opType.ordinal)
        buffer.putLong(operation.txId)
        buffer.putString(operation.index.components[1])
        buffer.putString(operation.index.components[2])
        buffer.putString(operation.index.components[3])
        buffer.putInt(operation.type.ordinal)
        buffer.putInt(operation.columns.size)
        for (col in operation.columns) {
            buffer.putString(col.simple)
        }
        buffer.putInt(operation.params.size)
        for ((k,v) in operation.params) {
            buffer.putString(k)
            buffer.putString(v)
        }
        return buffer
    }

    override fun deserialize(buffer: ByteBuffer): Operation.CreateIndexOperation {
        check(buffer.int == OperationType.CREATE_INDEX.ordinal)
        val txId = buffer.long
        val index = Name.IndexName(buffer.getString(), buffer.getString(), buffer.getString())
        val indexType = IndexType.values()[buffer.int]
        val columns = Array(buffer.int) {
            index.entity().column(buffer.getString())
        }
        val params = (0 until buffer.int).associate {
            buffer.getString() to buffer.getString()
        }
        return Operation.CreateIndexOperation(txId, index, indexType, columns, params)
    }
}