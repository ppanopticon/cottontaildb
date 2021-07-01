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
object DropEntityOperationSerializer: Serializer<Operation.DropEntityOperation>() {
    override fun sizeOf(operation: Operation.DropEntityOperation): Int =
        16 + operation.entity.components[1].length * Char.SIZE_BYTES +
             operation.entity.components[2].length * Char.SIZE_BYTES

    override fun serialize(operation: Operation.DropEntityOperation): ByteBuffer {
        val buffer = ByteBuffer.allocate(this.sizeOf(operation))
        buffer.putInt(operation.opType.ordinal)
        buffer.putLong(operation.txId)
        buffer.putString(operation.entity.components[1])
        buffer.putString(operation.entity.components[2])
        return buffer
    }
    override fun deserialize(buffer: ByteBuffer): Operation.DropEntityOperation {
        check(buffer.int == OperationType.DROP_ENTITY.ordinal)
        return Operation.DropEntityOperation(buffer.long, Name.EntityName(buffer.getString(), buffer.getString()))
    }
}