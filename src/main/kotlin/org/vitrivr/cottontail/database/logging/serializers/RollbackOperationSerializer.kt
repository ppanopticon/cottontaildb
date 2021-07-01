package org.vitrivr.cottontail.database.logging.serializers

import org.vitrivr.cottontail.database.logging.operations.Operation
import org.vitrivr.cottontail.database.logging.operations.OperationType
import java.nio.ByteBuffer

/**
 * A [Serializer] implementation for [Operation.RollbackOperation]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object RollbackOperationSerializer: Serializer<Operation.RollbackOperation>() {
    override fun sizeOf(operation: Operation.RollbackOperation): Int = 12
    override fun serialize(operation: Operation.RollbackOperation): ByteBuffer = ByteBuffer
        .allocate(this.sizeOf(operation))
        .putInt(operation.opType.ordinal)
        .putLong(operation.txId)

    override fun deserialize(buffer: ByteBuffer): Operation.RollbackOperation {
        check(buffer.int == OperationType.ROLLBACK.ordinal)
        return Operation.RollbackOperation(buffer.long)
    }
}