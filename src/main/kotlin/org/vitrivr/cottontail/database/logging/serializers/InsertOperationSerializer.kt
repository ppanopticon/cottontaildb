package org.vitrivr.cottontail.database.logging.serializers

import org.vitrivr.cottontail.database.logging.operations.Operation
import java.nio.ByteBuffer

/**
 * A [Serializer] implementation for [Operation.InsertOperationSerializer]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object InsertOperationSerializer : Serializer<Operation.DataManagementOperation.InsertOperation>() {
    override fun sizeOf(operation: Operation.DataManagementOperation.InsertOperation): Int {
        TODO("Not yet implemented")
    }

    override fun serialize(operation: Operation.DataManagementOperation.InsertOperation): ByteBuffer {
        val buffer = ByteBuffer.allocate(this.sizeOf(operation))
        buffer.putInt(operation.opType.ordinal)
        buffer.putLong(operation.txId)
        return buffer
    }

    override fun deserialize(buffer: ByteBuffer): Operation.DataManagementOperation.InsertOperation {
        TODO("Not yet implemented")
    }
}