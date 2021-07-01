package org.vitrivr.cottontail.database.logging.serializers

import org.vitrivr.cottontail.database.logging.operations.Operation
import org.vitrivr.cottontail.database.logging.operations.OperationType
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.utilities.extensions.getString
import org.vitrivr.cottontail.utilities.extensions.putString
import java.nio.ByteBuffer

/**
 * A [Serializer] implementation for [Operation.CreateSchemaOperation]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object CreateSchemaOperationSerializer: Serializer<Operation.CreateSchemaOperation>() {
    override fun sizeOf(operation: Operation.CreateSchemaOperation): Int = 16 + operation.schema.components[1].length * Char.SIZE_BYTES
    override fun serialize(operation: Operation.CreateSchemaOperation): ByteBuffer {
        val buffer = ByteBuffer.allocate(this.sizeOf(operation))
        buffer.putInt(operation.opType.ordinal)
        buffer.putLong(operation.txId)
        buffer.putString( operation.schema.components[1])
        return buffer
    }
    override fun deserialize(buffer: ByteBuffer): Operation.CreateSchemaOperation {
        check(buffer.int == OperationType.CREATE_SCHEMA.ordinal)
        return Operation.CreateSchemaOperation(buffer.long, Name.SchemaName(buffer.getString()))
    }
}