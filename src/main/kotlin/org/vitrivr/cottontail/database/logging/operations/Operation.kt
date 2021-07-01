package org.vitrivr.cottontail.database.logging.operations

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.logging.serializers.*
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TransactionId
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import java.nio.ByteBuffer

/**
 * A [Operation] that can be executed by the Cottontail DB database engine.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Operation(val txId: TransactionId) {

    /** The [OperationType] of this [Operation]. */
    abstract val opType: OperationType

    /**
     * Returns a [ByteBuffer] representation of this [Operation].
     *
     * @return [ByteBuffer]
     */
    abstract val serializer: Serializer<*>

    /**
     * [Operation] that captures a COMMIT
     */
    class CommitOperation(txId: TransactionId): Operation(txId) {
        override val opType: OperationType = OperationType.COMMIT
        override val serializer = CommitOperationSerializer
    }

    /**
     * [Operation] that captures a ROLLBACK
     */
    class RollbackOperation(txId: TransactionId): Operation(txId) {
        override val opType: OperationType = OperationType.ROLLBACK
        override val serializer = RollbackOperationSerializer
    }

    /**
     * [Operation] that captures the creation of a [org.vitrivr.cottontail.database.schema.Schema].
     */
    class CreateSchemaOperation(txId: TransactionId, val schema: Name.SchemaName): Operation(txId) {
        override val opType: OperationType = OperationType.CREATE_SCHEMA
        override val serializer = CreateSchemaOperationSerializer
    }

    /**
     * [Operation] that captures dropping of a [org.vitrivr.cottontail.database.schema.Schema].
     */
    class DropSchemaOperation(txId: TransactionId, val schema: Name.SchemaName): Operation(txId) {
        override val opType: OperationType = OperationType.DROP_SCHEMA
        override val serializer = DropSchemaOperationSerializer
    }

    /**
     * [Operation] that captures creation of an [org.vitrivr.cottontail.database.entity.Entity].
     */
    class CreateEntityOperation(txId: TransactionId, val entity: Name.EntityName, val columns: List<ColumnDef<*>>): Operation(txId) {
        override val opType: OperationType = OperationType.CREATE_ENTITY
        override val serializer: Serializer<*> = CreateEntityOperationSerializer
    }

    /**
     * [Operation] that captures dropping of an [org.vitrivr.cottontail.database.entity.Entity].
     */
    class DropEntityOperation(txId: TransactionId, val entity: Name.EntityName): Operation(txId) {
        override val opType: OperationType = OperationType.DROP_ENTITY
        override val serializer: Serializer<*> = DropEntityOperationSerializer
    }

    /**
     * [Operation] that captures creation of an [org.vitrivr.cottontail.database.index.basics.Index].
     */
    class CreateIndexOperation(txId: TransactionId, val index: Name.IndexName, val type: IndexType, val columns: Array<Name.ColumnName>, val params: Map<String, String> = emptyMap()): Operation(txId) {
        override val opType: OperationType = OperationType.CREATE_INDEX
        override val serializer: Serializer<*> = CreateIndexOperationSerializer
    }

    /**
     * [Operation] that captures creation of an [org.vitrivr.cottontail.database.index.basics.Index].
     */
    class DropIndexOperation(txId: TransactionId, val index: Name.IndexName): Operation(txId) {
        override val opType: OperationType = OperationType.DROP_INDEX
        override val serializer: Serializer<*> = DropIndexOperationSerializer
    }

    sealed class DataManagementOperation(txId: TransactionId, val entity: Name.EntityName, val tupleId: TupleId): Operation(txId) {

        /**
         * A [DataManagementOperation] that signals an INSERT into an [Entity]
         */
        class InsertOperation(txId: TransactionId, entity: Name.EntityName, tupleId: TupleId, val inserts: Map<Name.ColumnName, Value?>) : DataManagementOperation(txId, entity, tupleId) {
            override val opType: OperationType = OperationType.INSERT
            override val serializer: Serializer<*> = InsertOperationSerializer
        }

        /**
         * A [DataManagementOperation] that signals an UPDATE in an [Entity]
         */
        class UpdateOperation(txId: TransactionId, entity: Name.EntityName, tupleId: TupleId, val updates: Map<Name.ColumnName, Pair<Value?, Value?>>, ) : DataManagementOperation(txId, entity, tupleId) {
            override val opType: OperationType = OperationType.UPDATE
            override val serializer: Serializer<*> = TODO()
        }

        /**
         * A [DataManagementOperation] that signals a DELETE from an [Entity]
         */
        class DeleteOperation(txId: TransactionId, entity: Name.EntityName, tupleId: TupleId, val deleted: Map<Name.ColumnName, Value?>) : DataManagementOperation(txId, entity, tupleId) {
            override val opType: OperationType = OperationType.DELETE
            override val serializer: Serializer<*> = TODO()
        }
    }
}