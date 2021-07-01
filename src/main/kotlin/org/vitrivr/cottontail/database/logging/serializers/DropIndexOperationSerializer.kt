package org.vitrivr.cottontail.database.logging.serializers

import org.vitrivr.cottontail.database.logging.operations.Operation
import org.vitrivr.cottontail.database.logging.operations.OperationType
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.utilities.extensions.getString
import org.vitrivr.cottontail.utilities.extensions.putString
import java.nio.ByteBuffer

/**
 * A [Serializer] implementation for [Operation.DropSchemaOperation]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DropIndexOperationSerializer: Serializer<Operation.DropIndexOperation>() {
    override fun sizeOf(operation: Operation.DropIndexOperation): Int =
        16 + operation.index.components[1].length * Char.SIZE_BYTES +
            operation.index.components[2].length * Char.SIZE_BYTES +
            operation.index.components[3].length * Char.SIZE_BYTES

    override fun serialize(operation: Operation.DropIndexOperation): ByteBuffer {
        val buffer = ByteBuffer.allocate(this.sizeOf(operation))
        buffer.putInt(operation.opType.ordinal)
        buffer.putLong(operation.txId)
        buffer.putString(operation.index.components[1])
        buffer.putString(operation.index.components[2])
        buffer.putString(operation.index.components[3])
        return buffer
    }
    override fun deserialize(buffer: ByteBuffer): Operation.DropIndexOperation {
        check(buffer.int == OperationType.DROP_INDEX.ordinal)
        return Operation.DropIndexOperation(buffer.long, Name.IndexName(buffer.getString(), buffer.getString(), buffer.getString()))
    }
}