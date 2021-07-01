package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.single
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnEngine
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.binding.extensions.fqn
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.operators.definition.*
import org.vitrivr.cottontail.execution.operators.sort.HeapSortOperator
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpcKt
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import kotlin.time.ExperimentalTime

/**
 * This is a gRPC service endpoint that handles DDL (= Data Definition Language) request for Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@ExperimentalTime
class DDLService(val catalogue: DefaultCatalogue, override val manager: TransactionManager) : DDLGrpcKt.DDLCoroutineImplBase(), gRPCTransactionService {

    /**
     * gRPC endpoint for creating a new [org.vitrivr.cottontail.database.schema.Schema]
     */
    override suspend fun createSchema(request: CottontailGrpc.CreateSchemaMessage): CottontailGrpc.QueryResponseMessage = this.withTransactionContext(request.txId, "CREATE SCHEMA") { tx, q ->
        val schemaName = request.schema.fqn()
        val op = CreateSchemaOperator(this.catalogue, schemaName)
        executeAndMaterialize(tx, op, q, 0).catch { e ->
            throw when (e) {
                is DatabaseException.SchemaAlreadyExistsException -> Status.ALREADY_EXISTS.withDescription(formatMessage(tx, q, "CREATE SCHEMA failed ($schemaName): Schema with identical name already exists.")).asException()
                else -> e
            }
        }
    }.single()

    /**
     * gRPC endpoint for dropping a [org.vitrivr.cottontail.database.schema.Schema]
     */
    override suspend fun dropSchema(request: CottontailGrpc.DropSchemaMessage): CottontailGrpc.QueryResponseMessage = this.withTransactionContext(request.txId, "DROP SCHEMA") { tx, q ->
        val schemaName = request.schema.fqn()
        val op = DropSchemaOperator(this.catalogue, schemaName)
        executeAndMaterialize(tx, op, q, 0).catch { e ->
            throw when (e) {
                is DatabaseException.SchemaDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "DROP SCHEMA failed ($schemaName): Schema does not exist.")).asException()
                else -> e
            }
        }
    }.single()

    /**
     * gRPC endpoint listing the available [org.vitrivr.cottontail.database.schema.DefaultSchema]s.
     */
    override fun listSchemas(request: CottontailGrpc.ListSchemaMessage): Flow<CottontailGrpc.QueryResponseMessage> = this.withTransactionContext(request.txId, "LIST SCHEMA") { tx, q ->
        val op = HeapSortOperator(ListSchemaOperator(this.catalogue), arrayOf(Pair(ListSchemaOperator.COLUMNS[0], SortOrder.ASCENDING)), 100)
        executeAndMaterialize(tx, op, q, 0)
    }

    /**
     * gRPC endpoint for creating a new [org.vitrivr.cottontail.database.entity.Entity]
     */
    override suspend fun createEntity(request: CottontailGrpc.CreateEntityMessage): CottontailGrpc.QueryResponseMessage = this.withTransactionContext(request.txId, "CREATE ENTITY") { tx, q ->
        val entityName = request.definition.entity.fqn()
        val columns = request.definition.columnsList.map {
            val type = Type.forName(it.type.name, it.length)
            val name = entityName.column(it.name)
            ColumnDef(name, type, it.nullable) to ColumnEngine.valueOf(it.engine.toString())
        }.toTypedArray()
        val op = CreateEntityOperator(this.catalogue, entityName, columns)
        executeAndMaterialize(tx, op, q, 0).catch { e ->
            throw when (e) {
                is DatabaseException.SchemaDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "CREATE ENTITY failed ($entityName): Schema does not exist.")).asException()
                is DatabaseException.EntityAlreadyExistsException -> Status.ALREADY_EXISTS.withDescription(formatMessage(tx, q, "CREATE ENTITY failed ($entityName): Entity with identical name already exists.")).asException()
                else -> e
            }
        }
    }.single()

    /**
     * gRPC endpoint for dropping a specific [org.vitrivr.cottontail.database.entity.Entity].
     */
    override suspend fun dropEntity(request: CottontailGrpc.DropEntityMessage): CottontailGrpc.QueryResponseMessage = this.withTransactionContext(request.txId, "DROP ENTITY") { tx, q ->
        val entityName = request.entity.fqn()
        val op = DropEntityOperator(this.catalogue, entityName)
        executeAndMaterialize(tx, op, q, 0).catch { e ->
            throw when (e) {
                is DatabaseException.SchemaDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "DROP ENTITY failed ($entityName): Schema does not exist.")).asException()
                is DatabaseException.EntityDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "DROP ENTITY failed ($entityName): Entity does not exist.")).asException()
                else -> e
            }
        }
    }.single()

    /**
     * gRPC endpoint for truncating a specific [org.vitrivr.cottontail.database.entity.Entity].
     */
    override suspend fun truncateEntity(request: CottontailGrpc.TruncateEntityMessage): CottontailGrpc.QueryResponseMessage = this.withTransactionContext(request.txId, "TRUNCATE ENTITY") { tx, q ->
        val entityName = request.entity.fqn()
        val op = TruncateEntityOperator(this.catalogue, entityName)
        executeAndMaterialize(tx, op, q, 0).catch { e ->
            throw when (e) {
                is DatabaseException.SchemaDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "TRUNCATE ENTITY failed ($entityName): Schema does not exist.")).asException()
                is DatabaseException.EntityDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "TRUNCATE ENTITY failed ($entityName): Entity does not exist.")).asException()
                else -> e
            }
        }
    }.single()

    /**
     * gRPC endpoint for optimizing a particular [org.vitrivr.cottontail.database.entity.Entity].
     */
    override suspend fun optimizeEntity(request: CottontailGrpc.OptimizeEntityMessage): CottontailGrpc.QueryResponseMessage = this.withTransactionContext(request.txId, "OPTIMIZE ENTITY") { tx, q ->
        val entityName = request.entity.fqn()
        val op = OptimizeEntityOperator(this.catalogue, entityName)
        executeAndMaterialize(tx, op, q, 0).catch { e ->
            throw when (e) {
                is DatabaseException.SchemaDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "OPTIMIZE ENTITY failed ($entityName): Schema does not exist.")).asException()
                is DatabaseException.EntityDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "OPTIMIZE ENTITY failed ($entityName): Entity does not exist.")).asException()
                else -> e
            }
        }
    }.single()

    /**
     * gRPC endpoint listing the available [org.vitrivr.cottontail.database.entity.Entity]s for the provided [org.vitrivr.cottontail.database.schema.Schema].
     */
    override fun listEntities(request: CottontailGrpc.ListEntityMessage): Flow<CottontailGrpc.QueryResponseMessage> = this.withTransactionContext(request.txId, "LIST ENTITIES") { tx, q ->
        val schemaName = if (request.hasSchema()) {
            request.schema.fqn()
        } else {
            null
        }
        val op = HeapSortOperator(ListEntityOperator(this.catalogue, schemaName), arrayOf(Pair(ListSchemaOperator.COLUMNS[0], SortOrder.ASCENDING)), 100)
        executeAndMaterialize(tx, op, q, 0).catch { e ->
            throw when (e) {
                is DatabaseException.SchemaDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "LIST ENTITIES failed ($schemaName}': Schema does not exist.")).asException()
                else -> e
            }
        }
    }

    /**
     * gRPC endpoint for requesting details about a specific [org.vitrivr.cottontail.database.entity.Entity].
     */
    override suspend fun entityDetails(request: CottontailGrpc.EntityDetailsMessage): CottontailGrpc.QueryResponseMessage = this.withTransactionContext(request.txId, "SHOW ENTITY") { tx, q ->
        val entityName = request.entity.fqn()
        val op = EntityDetailsOperator(this.catalogue, entityName)
        executeAndMaterialize(tx, op, q, 0).catch { e ->
            throw when (e) {
                is DatabaseException.SchemaDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "SHOW ENTITY failed ($entityName): Schema does not exist.")).asException()
                is DatabaseException.EntityDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "SHOW ENTITY failed ($entityName): Entity does not exist.")).asException()
                else -> e
            }
        }
    }.single()

    /**
     * gRPC endpoint for creating a particular [org.vitrivr.cottontail.database.index.Index]
     */
    override suspend fun createIndex(request: CottontailGrpc.CreateIndexMessage): CottontailGrpc.QueryResponseMessage = this.withTransactionContext(request.txId, "CREATE INDEX") { tx, q ->
        val indexName = request.definition.name.fqn()
        val columns = request.definition.columnsList.map {
            indexName.entity().column(it.name)
        }
        val indexType = IndexType.valueOf(request.definition.type.toString())
        val params = request.definition.paramsMap
        val op = CreateIndexOperator(this.catalogue, indexName, indexType, columns, params, request.rebuild)
        executeAndMaterialize(tx, op, q, 0).catch { e ->
            throw when (e) {
                is DatabaseException.SchemaDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "CREATE INDEX failed ($indexName): Schema does not exist.")).asException()
                is DatabaseException.EntityDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "CREATE INDEX failed ($indexName): Entity does not exist.")).asException()
                is DatabaseException.ColumnDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "CREATE INDEX failed ($indexName): Column does not exist.")).asException()
                is DatabaseException.IndexAlreadyExistsException -> Status.ALREADY_EXISTS.withDescription(formatMessage(tx, q, "CREATE INDEX failed ($indexName): Index with identical name does already exist.")).asException()

                else -> e
            }
        }
    }.single()

    /**
     * gRPC endpoint for dropping a particular [org.vitrivr.cottontail.database.index.Index]
     */
    override suspend fun dropIndex(request: CottontailGrpc.DropIndexMessage): CottontailGrpc.QueryResponseMessage = this.withTransactionContext(request.txId, "DROP INDEX") { tx, q ->
        val indexName = request.index.fqn()
        val op = DropIndexOperator(this.catalogue, indexName)
        executeAndMaterialize(tx, op, q, 0).catch { e ->
            throw when (e) {
                is DatabaseException.SchemaDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "DROP INDEX failed ($indexName): Schema does not exist.")).asException()
                is DatabaseException.EntityDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "DROP INDEX failed ($indexName): Entity does not exist.")).asException()
                is DatabaseException.IndexDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "DROP INDEX failed ($indexName): Index does not exist.")).asException()
                else -> e
            }
        }
    }.single()

    /**
     * gRPC endpoint for rebuilding a particular [org.vitrivr.cottontail.database.index.Index]
     */
    override suspend fun rebuildIndex(request: CottontailGrpc.RebuildIndexMessage): CottontailGrpc.QueryResponseMessage = this.withTransactionContext(request.txId, "REBUILD INDEX") { tx, q ->
        val indexName = request.index.fqn()
        val op = RebuildIndexOperator(this.catalogue, indexName)
        executeAndMaterialize(tx, op, q, 0).catch { e ->
            throw when (e) {
                is DatabaseException.SchemaDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "REBUILD INDEX failed ($indexName): Schema does not exist.")).asException()
                is DatabaseException.EntityDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "REBUILD INDEX failed ($indexName): Entity does not exist.")).asException()
                is DatabaseException.IndexDoesNotExistException -> Status.NOT_FOUND.withDescription(formatMessage(tx, q, "REBUILD INDEX failed ($indexName): Index does not exist.")).asException()
                else -> e
            }
        }
    }.single()
}
