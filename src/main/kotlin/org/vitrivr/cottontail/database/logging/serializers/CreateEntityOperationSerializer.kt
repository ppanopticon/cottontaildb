package org.vitrivr.cottontail.database.logging.serializers

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.logging.operations.Operation
import org.vitrivr.cottontail.database.logging.operations.OperationType
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.utilities.extensions.getString
import org.vitrivr.cottontail.utilities.extensions.putString
import org.vitrivr.cottontail.utilities.extensions.toByte
import java.nio.ByteBuffer

/**
 * A [Serializer] implementation for [Operation.CreateEntityOperation]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object CreateEntityOperationSerializer: Serializer<Operation.CreateEntityOperation>() {
    override fun sizeOf(operation: Operation.CreateEntityOperation): Int = 12 +
        (4 + operation.entity.components[1].length * Char.SIZE_BYTES) +
        (4 + operation.entity.components[2].length * Char.SIZE_BYTES) +
        4 + operation.columns.sumOf { 12 + 4 + (it.name.simple.length * Char.SIZE_BYTES) }

    override fun serialize(operation: Operation.CreateEntityOperation): ByteBuffer {
        val buffer = ByteBuffer.allocate(this.sizeOf(operation))
        buffer.putInt(operation.opType.ordinal)
        buffer.putLong(operation.txId)
        buffer.putString(operation.entity.components[1])
        buffer.putString(operation.entity.components[2])
        buffer.putInt(operation.columns.size)
        for (col in operation.columns) {
            buffer.putString(col.name.simple)
            buffer.putInt(col.type.ordinal)
            buffer.putInt(col.type.logicalSize)
            buffer.put(col.nullable.toByte())
            buffer.put(col.primary.toByte())
        }
        return buffer
    }

    override fun deserialize(buffer: ByteBuffer): Operation.CreateEntityOperation {
        check(buffer.int == OperationType.CREATE_ENTITY.ordinal)
        val txId = buffer.long
        val entity = Name.EntityName(buffer.getString(), buffer.getString())
        val columns = (0 until buffer.int).map {
            ColumnDef(entity.column(buffer.getString()), Type.forOrdinal(buffer.int, buffer.int), buffer.get() == 1.toByte(),buffer.get() == 1.toByte())
        }
        return Operation.CreateEntityOperation(txId, entity, columns)
    }
}