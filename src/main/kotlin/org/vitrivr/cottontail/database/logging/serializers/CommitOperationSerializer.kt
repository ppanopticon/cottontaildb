package org.vitrivr.cottontail.database.logging.serializers

import org.vitrivr.cottontail.database.logging.operations.Operation
import org.vitrivr.cottontail.database.logging.operations.OperationType
import java.nio.ByteBuffer

/**
 * A [Serializer] implementation for [Operation.CommitOperation]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object CommitOperationSerializer: Serializer<Operation.CommitOperation>() {
    override fun sizeOf(operation: Operation.CommitOperation): Int = 12
    override fun serialize(operation: Operation.CommitOperation): ByteBuffer = ByteBuffer
        .allocate(this.sizeOf(operation))
        .putInt(operation.opType.ordinal)
        .putLong(operation.txId)

    override fun deserialize(buffer: ByteBuffer): Operation.CommitOperation {
        check(buffer.int == OperationType.COMMIT.ordinal)
        return Operation.CommitOperation(buffer.long)
    }
}