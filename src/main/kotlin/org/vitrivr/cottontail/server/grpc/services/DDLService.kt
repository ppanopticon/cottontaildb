package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnEngine
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.binding.extensions.fqn
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.operators.definition.*
import org.vitrivr.cottontail.execution.operators.sinks.SpoolerSinkOperator
import org.vitrivr.cottontail.execution.operators.sort.HeapSortOperator
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.ExecutionException
import org.vitrivr.cottontail.model.exceptions.TransactionException
import java.util.*
import kotlin.time.ExperimentalTime

/**
 * This is a gRPC service endpoint that handles DDL (= Data Definition Language) request for Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
@ExperimentalTime
class DDLService(val catalogue: DefaultCatalogue, override val manager: TransactionManager) : DDLGrpc.DDLImplBase(), TransactionService {

    /**
     * gRPC endpoint listing the available [org.vitrivr.cottontail.database.schema.DefaultSchema]s.
     */
    override fun listSchemas(request: CottontailGrpc.ListSchemaMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId, responseObserver) { tx, q ->
        try {
            val op = SpoolerSinkOperator(
                HeapSortOperator(ListSchemaOperator(this.catalogue), arrayOf(Pair(ListSchemaOperator.COLUMNS[0], SortOrder.ASCENDING)), 100), q, 0, responseObserver
            )
            tx.execute(op)
            Status.OK
        } catch (e: TransactionException.DeadlockException) {
            Status.ABORTED.withDescription(formatMessage(tx, q, "Failed to fetch list of schemas because of a deadlock with another transaction."))
        } catch (e: ExecutionException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Failed to fetch list of schemas because of a database error.")).withCause(e)
        } catch (e: Throwable) {
            Status.UNKNOWN.withDescription(formatMessage(tx, q, "Failed to fetch list of schemas because of an unexpected error.")).withCause(e)
        }
    }

    /**
     * gRPC endpoint for creating a new [org.vitrivr.cottontail.database.schema.DefaultSchema]
     */
    override fun createSchema(request: CottontailGrpc.CreateSchemaMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId, responseObserver) function@{ tx, q ->
        val schemaName = request.schema.fqn()
        try {
            /* Execute operation. */
            val op = SpoolerSinkOperator(CreateSchemaOperator(this.catalogue, request.schema.fqn()), q, 0, responseObserver)
            tx.execute(op)
            Status.OK.withDescription(formatMessage(tx, q, "Schema '$schemaName' created successfully!"))
        } catch (e: DatabaseException.SchemaAlreadyExistsException) {
            Status.ALREADY_EXISTS.withDescription(formatMessage(tx, q, "Failed to create schema '${request.schema.fqn()}': Schema with identical name already exists."))
        } catch (e: TransactionException.DeadlockException) {
            Status.ABORTED.withDescription(formatMessage(tx, q, "Failed to create schema '${request.schema.fqn()}' because of a deadlock with another transaction."))
        } catch (e: ExecutionException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Failed to create schema '${request.schema.fqn()}' because of a database error.")).withCause(e)
        } catch (e: Throwable) {
            Status.UNKNOWN.withDescription(formatMessage(tx, q, "Failed to create schema '${request.schema.fqn()}' because of an unexpected error.")).withCause(e)
        }
    }

    /**
     * gRPC endpoint for dropping a [org.vitrivr.cottontail.database.schema.DefaultSchema]
     */
    override fun dropSchema(request: CottontailGrpc.DropSchemaMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId, responseObserver) { tx, q ->
        /* Obtain transaction or create new one. */
        val schemaName = request.schema.fqn()
        try {
            /* Execution operation. */
            tx.execute(SpoolerSinkOperator(DropSchemaOperator(this.catalogue, schemaName), q, 0, responseObserver))
            Status.OK.withDescription(formatMessage(tx, q, "Schema '$schemaName' dropped successfully!"))
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to drop schema '${request.schema.fqn()}': Schema does not exist."))
        } catch (e: TransactionException.DeadlockException) {
            Status.ABORTED.withDescription(formatMessage(tx, q, "Failed to drop schema'${request.schema.fqn()}' because of a deadlock with another transaction."))
        } catch (e: ExecutionException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Failed to drop schema '${request.schema.fqn()}' because of a database error.")).withCause(e)
        } catch (e: Throwable) {
            Status.UNKNOWN.withDescription(formatMessage(tx, q, "Failed to drop schema '${request.schema.fqn()}' because of an unexpected error.")).withCause(e)
        }
    }

    /**
     * gRPC endpoint for requesting details about a specific [org.vitrivr.cottontail.database.entity.DefaultEntity].
     */
    override fun entityDetails(request: CottontailGrpc.EntityDetailsMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId, responseObserver) { tx, q ->
        /* Obtain transaction or create new one. */
        val entityName = request.entity.fqn()
        val queryId = UUID.randomUUID().toString()

        try {
            /* Execution operation. */
            val op = SpoolerSinkOperator(EntityDetailsOperator(this.catalogue, entityName), queryId, 0, responseObserver)
            tx.execute(op)
            Status.OK
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to fetch entity information for '${request.entity.fqn()}': Schema does not exist."))
        } catch (e: DatabaseException.EntityDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to fetch entity information for '${request.entity.fqn()}': Entity does not exist."))
        } catch (e: TransactionException.DeadlockException) {
            Status.ABORTED.withDescription(formatMessage(tx, q, "Failed to fetch entity information for '${request.entity.fqn()}' because of a deadlock with another transaction."))
        } catch (e: ExecutionException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Failed to fetch entity information '${request.entity.fqn()}' because of a database error.")).withCause(e)
        } catch (e: Throwable) {
            Status.UNKNOWN.withDescription(formatMessage(tx, q, "Failed to fetch entity information '${request.entity.fqn()}' because of an unexpected error.")).withCause(e)
        }
    }

    /**
     * gRPC endpoint for creating a new [org.vitrivr.cottontail.database.entity.DefaultEntity]
     */
    override fun createEntity(request: CottontailGrpc.CreateEntityMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId, responseObserver) { tx, q ->
        /* Obtain transaction or create new one. */
        val entityName = request.definition.entity.fqn()

        try {
            val columns = request.definition.columnsList.map {
                val type = Type.forName(it.type.name, it.length)
                val name = entityName.column(it.name)
                ColumnDef(name, type, it.nullable) to ColumnEngine.valueOf(it.engine.toString())
            }.toTypedArray()

            /* Execution operation. */
            val op = SpoolerSinkOperator(CreateEntityOperator(this.catalogue, entityName, columns), q, 0, responseObserver)
            tx.execute(op)

            /* Finalize invocation. */
            Status.OK.withDescription(formatMessage(tx, q, "Schema '$entityName' created successfully!"))
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to create entity '${request.definition.entity.fqn()}': Schema does not exist."))
        } catch (e: DatabaseException.EntityAlreadyExistsException) {
            Status.ALREADY_EXISTS.withDescription(formatMessage(tx, q, "Failed to create entity '${request.definition.entity.fqn()}': Entity with identical name already exists."))
        } catch (e: TransactionException.DeadlockException) {
            Status.ABORTED.withDescription(formatMessage(tx, q, "Failed to create entity '${request.definition.entity.fqn()}' because of a deadlock with another transaction."))
        } catch (e: ExecutionException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Failed to create entity '${request.definition.entity.fqn()}' because of a database error.")).withCause(e)
        } catch (e: Throwable) {
            Status.UNKNOWN.withDescription(formatMessage(tx, q, "Failed to create entity '${request.definition.entity.fqn()}' because of an unexpected error.")).withCause(e)
        }
    }

    /**
     * gRPC endpoint for dropping a specific [org.vitrivr.cottontail.database.entity.DefaultEntity].
     */
    override fun dropEntity(request: CottontailGrpc.DropEntityMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId, responseObserver) { tx, q ->
        val entityName = request.entity.fqn()
        try {
            /* Execution operation. */
            val op = SpoolerSinkOperator(DropEntityOperator(this.catalogue, entityName), q, 0, responseObserver)
            tx.execute(op)

            /* Finalize invocation. */
            Status.OK.withDescription(formatMessage(tx, q, "Entity '$entityName' dropped successfully!"))
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to drop entity '${request.entity.fqn()}': Schema does not exist."))
        } catch (e: DatabaseException.EntityDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to drop entity '${request.entity.fqn()}': Entity does not exist."))
        } catch (e: TransactionException.DeadlockException) {
            Status.ABORTED.withDescription(formatMessage(tx, q, "Failed to drop entity '${request.entity.fqn()}' because of a deadlock with another transaction."))
        } catch (e: ExecutionException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Failed to drop entity '${request.entity.fqn()}' because of a database error.")).withCause(e)
        } catch (e: Throwable) {
            Status.UNKNOWN.withDescription(formatMessage(tx, q, "Failed to drop entity '${request.entity.fqn()}' because of an unexpected error.")).withCause(e)
        }
    }

    /**
     * gRPC endpoint for truncating a specific [org.vitrivr.cottontail.database.entity.DefaultEntity].
     */
    override fun truncateEntity(request: CottontailGrpc.TruncateEntityMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId, responseObserver) { tx, q ->
        val entityName = request.entity.fqn()
        try {
            /* Execution operation. */
            val op = SpoolerSinkOperator(TruncateEntityOperator(this.catalogue, entityName), q, 0, responseObserver)
            tx.execute(op)

            /* Finalize invocation. */
            Status.OK.withDescription(formatMessage(tx, q, "Entity '$entityName' truncated successfully!"))
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to truncate entity '${request.entity.fqn()}': Schema does not exist."))
        } catch (e: DatabaseException.EntityDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to truncate entity '${request.entity.fqn()}': Entity does not exist."))
        } catch (e: TransactionException.DeadlockException) {
            Status.ABORTED.withDescription(formatMessage(tx, q, "Failed to truncate entity '${request.entity.fqn()}' because of a deadlock with another transaction."))
        } catch (e: ExecutionException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Failed to truncate entity '${request.entity.fqn()}' because of a database error.")).withCause(e)
        } catch (e: Throwable) {
            Status.UNKNOWN.withDescription(formatMessage(tx, q, "Failed to truncate entity '${request.entity.fqn()}' because of an unexpected error.")).withCause(e)
        }
    }

    /**
     * gRPC endpoint for optimizing a particular entity. Currently just rebuilds all the indexes.
     */
    override fun optimizeEntity(request: CottontailGrpc.OptimizeEntityMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId, responseObserver) { tx, q ->
        val entityName = request.entity.fqn()
        try {
            /* Execution operation. */
            val op = SpoolerSinkOperator(OptimizeEntityOperator(this.catalogue, entityName), q, 0, responseObserver)
            tx.execute(op)

            /* Finalize invocation. */
            Status.OK.withDescription(formatMessage(tx, q, "Entity '$entityName' optimized successfully!"))
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to optimize entity '${request.entity.fqn()}': Schema does not exist."))
        } catch (e: DatabaseException.EntityDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to optimize entity '${request.entity.fqn()}': Entity does not exist."))
        } catch (e: TransactionException.DeadlockException) {
            Status.ABORTED.withDescription(formatMessage(tx, q, "Failed to optimize entity '${request.entity.fqn()}' because of a deadlock with another transaction."))
        } catch (e: ExecutionException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Failed to truncate entity '${request.entity.fqn()}' because of a database error.")).withCause(e)
        } catch (e: Throwable) {
            Status.UNKNOWN.withDescription(formatMessage(tx, q, "Failed to truncate entity '${request.entity.fqn()}' because of an unexpected error.")).withCause(e)
        }
    }

    /**
     * gRPC endpoint listing the available [org.vitrivr.cottontail.database.entity.DefaultEntity]s for the provided [org.vitrivr.cottontail.database.schema.DefaultSchema].
     */
    override fun listEntities(request: CottontailGrpc.ListEntityMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId, responseObserver) { tx, q ->
        /* Extract schema name. */
        val schemaName = if (request.hasSchema()) {
            request.schema.fqn()
        } else {
            null
        }

        try {
            /* Execution operation. */
            val op = SpoolerSinkOperator(HeapSortOperator(ListEntityOperator(this.catalogue, schemaName), arrayOf(Pair(ListSchemaOperator.COLUMNS[0], SortOrder.ASCENDING)), 100), q, 0, responseObserver)
            tx.execute(op)

            /* Finalize invocation. */
            Status.OK
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to list entities for schema '${request.schema.fqn()}': Schema does not exist."))
        } catch (e: TransactionException.DeadlockException) {
            Status.ABORTED.withDescription(formatMessage(tx, q, "Failed to list entities for schema '${request.schema.fqn()}' because of a deadlock with another transaction."))
        } catch (e: ExecutionException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Failed to list entities for schema '${request.schema.fqn()}' because of a database error.")).withCause(e)
        } catch (e: Throwable) {
            Status.UNKNOWN.withDescription(formatMessage(tx, q, "Failed to list entities for schema '${request.schema.fqn()}' because of an unexpected error.")).withCause(e)
        }
    }

    /**
     * gRPC endpoint for creating a particular [org.vitrivr.cottontail.database.index.AbstractIndex]
     */
    override fun createIndex(request: CottontailGrpc.CreateIndexMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId, responseObserver) { tx, q ->
        try {
            /* Parses the CreateIndexMessage message. */
            val indexName = request.definition.name.fqn()
            val columns = request.definition.columnsList.map {
                indexName.entity().column(it.name)
            }
            val indexType = IndexType.valueOf(request.definition.type.toString())
            val params = request.definition.paramsMap

            /* Execution operation. */
            val createOp = SpoolerSinkOperator(CreateIndexOperator(this.catalogue, indexName, indexType, columns, params, request.rebuild), q, 0, responseObserver)
            tx.execute(createOp)

            /* Finalize invocation. */
            Status.OK.withDescription(formatMessage(tx, q, "Index '$indexName' created successfully!"))
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to create index '${request.definition.name.fqn()}': Schema does not exist."))
        } catch (e: DatabaseException.EntityDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to create index '${request.definition.name.fqn()}': Entity does not exist."))
        } catch (e: DatabaseException.ColumnDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to create index '${request.definition.name.fqn()}': Column does not exist."))
        } catch (e: DatabaseException.IndexAlreadyExistsException) {
            Status.ALREADY_EXISTS.withDescription(formatMessage(tx, q, "Failed to create index '${request.definition.name.fqn()}': Index with identical name does already exist."))
        } catch (e: TransactionException.DeadlockException) {
            Status.ABORTED.withDescription(formatMessage(tx, q, "Failed to list entities for schema '${request.definition.name.fqn()}' because of a deadlock with another transaction."))
        } catch (e: ExecutionException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Failed to create index '${request.definition.name.fqn()}' because of a database error.")).withCause(e)
        } catch (e: Throwable) {
            Status.UNKNOWN.withDescription(formatMessage(tx, q, "Failed to create index '${request.definition.name.fqn()}' because of an unexpected error.")).withCause(e)
        }
    }

    /**
     * gRPC endpoint for dropping a particular [org.vitrivr.cottontail.database.index.AbstractIndex]
     */
    override fun dropIndex(request: CottontailGrpc.DropIndexMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId, responseObserver) { tx, q ->
        try {
            /* Parses the DropIndexMessage message. */
            val indexName = request.index.fqn()

            /* Execution operation. */
            val op = SpoolerSinkOperator(DropIndexOperator(this.catalogue, indexName), q, 0, responseObserver)
            tx.execute(op)

            /* Notify caller of success. */
            Status.OK.withDescription(formatMessage(tx, q, "Index '$indexName' dropped successfully!"))
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to drop index '${request.index.fqn()}': Schema does not exist."))
        } catch (e: DatabaseException.EntityDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to drop index '${request.index.fqn()}': Entity does not exist."))
        } catch (e: DatabaseException.IndexDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to drop index '${request.index.fqn()}': Index does not exist."))
        } catch (e: TransactionException.DeadlockException) {
            Status.ABORTED.withDescription(formatMessage(tx, q, "Failed to list entities for schema '${request.index.fqn()}' because of a deadlock with another transaction."))
        } catch (e: ExecutionException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Failed to drop index '${request.index.fqn()}' because of a database error.")).withCause(e)
        } catch (e: Throwable) {
            Status.UNKNOWN.withDescription(formatMessage(tx, q, "Failed to drop index '${request.index.fqn()}' because of an unexpected error.")).withCause(e)
        }
    }

    /**
     * gRPC endpoint for rebuilding a particular [org.vitrivr.cottontail.database.index.AbstractIndex]
     */
    override fun rebuildIndex(request: CottontailGrpc.RebuildIndexMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId, responseObserver) { tx, q ->
        try {
            /* Parses the RebuildIndexMessage message. */
            val indexName = request.index.fqn()

            /* Execution operation. */
            val op = SpoolerSinkOperator(RebuildIndexOperator(this.catalogue, indexName), q, 0, responseObserver)
            tx.execute(op)

            /* Notify caller of success. */
            responseObserver.onCompleted()
            Status.OK.withDescription(formatMessage(tx, q, "Index '$indexName' rebuilt successfully!"))
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to rebuild index '${request.index.fqn()}': Schema does not exist."))
        } catch (e: DatabaseException.EntityDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to rebuild index '${request.index.fqn()}': Entity does not exist."))
        } catch (e: DatabaseException.IndexDoesNotExistException) {
            Status.NOT_FOUND.withDescription(formatMessage(tx, q, "Failed to rebuild index '${request.index.fqn()}': Index does not exist."))
        } catch (e: TransactionException.DeadlockException) {
            Status.ABORTED.withDescription(formatMessage(tx, q, "Failed to list entities for schema '${request.index.fqn()}' because of a deadlock with another transaction."))
        } catch (e: DatabaseException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Failed to rebuild index '${request.index.fqn()}' because of a database error.")).withCause(e)
        } catch (e: Throwable) {
            Status.UNKNOWN.withDescription(formatMessage(tx, q, "Failed to rebuild index '${request.index.fqn()}' because of an unexpected error.")).withCause(e)
        }
    }
}
