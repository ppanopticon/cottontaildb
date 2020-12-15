package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.column.ColumnType
import org.vitrivr.cottontail.database.locking.DeadlockException
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionStatus
import org.vitrivr.cottontail.execution.operators.definition.*
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.server.grpc.helper.fqn
import org.vitrivr.cottontail.server.grpc.operators.SpoolerSinkOperator
import java.util.*
import kotlin.time.ExperimentalTime

/**
 * This is a gRPC service endpoint that handles DDL (= Data Definition Language) request for Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
@ExperimentalTime
class DDLService(val catalogue: Catalogue, override val manager: TransactionManager) : DDLGrpc.DDLImplBase(), TransactionService {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(DDLService::class.java)
    }

    /**
     * gRPC endpoint listing the available [org.vitrivr.cottontail.database.schema.Schema]s.
     */
    override fun listSchemas(request: CottontailGrpc.ListSchemaMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) {
        /* Obtain transaction or create new one. */
        val queryId = UUID.randomUUID().toString()
        /* Obtain transaction or create new one. */
        val commitDirectly = !request.hasTxId()
        val transaction = if (commitDirectly) {
            this.manager.Transaction()
        } else {
            this.resumeTransaction(request.txId, responseObserver) ?: return
        }

        try {
            /* Prepare and execute transaction. */
            val op = SpoolerSinkOperator(ListSchemaOperator(this.catalogue), queryId, 0, responseObserver)
            transaction.execute(op)

            /* Finalize transaction. */
            if (commitDirectly && transaction.state == TransactionStatus.OPEN) {
                transaction.commit()
            }
            responseObserver.onCompleted()
        } catch (e: DatabaseException) {
            val message = "Failed to fetch list of schemas because of a database error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DeadlockException) {
            val message = "Failed to fetch list of schemas because of a deadlock with another transaction."
            LOGGER.info(message)
            responseObserver.onError(Status.ABORTED.withDescription(message).asException())
            transaction.rollback()
        } catch (e: Throwable) {
            val message = "Failed to fetch list of schemas because of an unknown error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
            transaction.rollback()
        }
    }

    /**
     * gRPC endpoint for creating a new [org.vitrivr.cottontail.database.schema.Schema]
     */
    override fun createSchema(request: CottontailGrpc.CreateSchemaMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) {
        /* Obtain transaction or create new one. */
        val schemaName = request.schema.fqn()
        val queryId = UUID.randomUUID().toString()

        /* Obtain transaction or create new one. */
        val commitDirectly = !request.hasTxId()
        val transaction = if (commitDirectly) {
            this.manager.Transaction()
        } else {
            this.resumeTransaction(request.txId, responseObserver) ?: return
        }

        try {
            /* Execute operation. */
            LOGGER.info("Creating schema '$schemaName'...")
            val op = SpoolerSinkOperator(CreateSchemaOperator(this.catalogue, schemaName), queryId, 0, responseObserver)
            transaction.execute(op)

            /* Finalize transaction. */
            if (commitDirectly && transaction.state == TransactionStatus.OPEN) {
                transaction.commit()
                LOGGER.info("Schema '$schemaName' created successfully!")
            }  else {
                LOGGER.info("Schema '$schemaName' created successfully (pending commit)!")
            }
            responseObserver.onCompleted()
        } catch (e: DatabaseException.EntityAlreadyExistsException) {
            val message = "Failed to create schema '${request.schema.fqn()}': Schema with identical name already exists."
            LOGGER.info(message)
            responseObserver.onError(Status.ALREADY_EXISTS.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DeadlockException) {
            val message = "Failed to create schema '${request.schema.fqn()}': Deadlock with another transaction."
            LOGGER.info(message)
            responseObserver.onError(Status.ABORTED.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DatabaseException) {
            val message = "Failed to create schema '${request.schema.fqn()}' because of a database error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            transaction.rollback()
        } catch (e: Throwable) {
            val message = "Failed to create schema '${request.schema.fqn()}' because of an unknown error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
            transaction.rollback()
        }
    }

    /**
     * gRPC endpoint for dropping a [org.vitrivr.cottontail.database.schema.Schema]
     */
    override fun dropSchema(request: CottontailGrpc.DropSchemaMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) {
        /* Obtain transaction or create new one. */
        val schemaName = request.schema.fqn()
        val queryId = UUID.randomUUID().toString()

        /* Obtain transaction or create new one. */
        val commitDirectly = !request.hasTxId()
        val transaction = if (commitDirectly) {
            this.manager.Transaction()
        } else {
            this.resumeTransaction(request.txId, responseObserver) ?: return
        }

        try {
            LOGGER.info("Dropping schema '$schemaName'...")

            /* Execution operation. */
            val op = SpoolerSinkOperator(DropSchemaOperator(this.catalogue, schemaName), queryId, 0, responseObserver)
            transaction.execute(op)

            /* Finalize transaction. */
            if (commitDirectly && transaction.state == TransactionStatus.OPEN) {
                transaction.commit()
                LOGGER.info("Schema '$schemaName' dropped successfully!")
            }  else {
                LOGGER.info("Schema '$schemaName' dropped successfully (pending commit)!")
            }
            responseObserver.onCompleted()
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            val message = "Failed to drop schema '${request.schema.fqn()}': Schema does not exist."
            LOGGER.info(message)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DeadlockException) {
            val message = "Failed to drop schema'${request.schema.fqn()}': Deadlock with another transaction."
            LOGGER.info(message)
            responseObserver.onError(Status.ABORTED.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DatabaseException) {
            val message = "Failed to drop schema '${request.schema.fqn()}' because of a database error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            transaction.rollback()
        } catch (e: Throwable) {
            val message = "Failed to drop schema '${request.schema.fqn()}' because of an unknown error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
            transaction.rollback()
        }
    }

    /**
     * gRPC endpoint for requesting details about a specific [org.vitrivr.cottontail.database.entity.Entity].
     */
    override fun entityDetails(request: CottontailGrpc.EntityDetailsMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) {
        /* Obtain transaction or create new one. */
        val entityName = request.entity.fqn()
        val queryId = UUID.randomUUID().toString()

        /* Obtain transaction or create new one. */
        val commitDirectly = !request.hasTxId()
        val transaction = if (commitDirectly) {
            this.manager.Transaction()
        } else {
            this.resumeTransaction(request.txId, responseObserver) ?: return
        }

        try {
            /* Execution operation. */
            val op = SpoolerSinkOperator(EntityDetailsOperator(this.catalogue, entityName), queryId, 0, responseObserver)
            transaction.execute(op)

            /* Finalize transaction. */
            if (commitDirectly && transaction.state == TransactionStatus.OPEN) {
                transaction.commit()
            }
            responseObserver.onCompleted()
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            val message = "Failed to fetch entity information for '${request.entity.fqn()}': Schema does not exist."
            LOGGER.info(message)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
        } catch (e: DatabaseException.EntityDoesNotExistException) {
            val message = "Failed to fetch entity information for '${request.entity.fqn()}': Entity does not exist."
            LOGGER.info(message)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
        } catch (e: DeadlockException) {
            val message = "Failed to fetch entity information for '${request.entity.fqn()}': Deadlock with another transaction."
            LOGGER.info(message)
            responseObserver.onError(Status.ABORTED.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DatabaseException) {
            val message = "Failed to fetch entity information '${request.entity.fqn()}' because of a database error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
        } catch (e: Throwable) {
            val message = "Failed to fetch entity information '${request.entity.fqn()}' because of an unknown error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
        }
    }

    /**
     * gRPC endpoint for creating a new [org.vitrivr.cottontail.database.entity.Entity]
     */
    override fun createEntity(request: CottontailGrpc.CreateEntityMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) {
        /* Obtain transaction or create new one. */
        val entityName = request.definition.entity.fqn()
        val queryId = UUID.randomUUID().toString()

        /* Obtain transaction or create new one. */
        val commitDirectly = !request.hasTxId()
        val transaction = if (commitDirectly) {
            this.manager.Transaction()
        } else {
            this.resumeTransaction(request.txId, responseObserver) ?: return
        }

        try {
            LOGGER.info("Creating entity '$entityName'...")
            val columns = request.definition.columnsList.map {
                val type = ColumnType.forName(it.type.name)
                val name = entityName.column(it.name)
                ColumnDef(name, type, it.length, it.nullable)
            }.toTypedArray()

            /* Execution operation. */
            val op = SpoolerSinkOperator(CreateEntityOperator(this.catalogue, entityName, columns), queryId, 0, responseObserver)
            transaction.execute(op)

            /* Finalize transaction. */
            if (commitDirectly && transaction.state == TransactionStatus.OPEN) {
                transaction.commit()
                LOGGER.info("Schema '$entityName' created successfully!")
            }  else {
                LOGGER.info("Schema '$entityName' created successfully (pending commit)!")
            }
            responseObserver.onCompleted()
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            val message = "Failed to create entity '${request.definition.entity.fqn()}': Schema does not exist."
            LOGGER.info(message)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DatabaseException.EntityAlreadyExistsException) {
            val message = "Failed to create entity '${request.definition.entity.fqn()}': Entity with identical name already exists."
            LOGGER.info(message)
            responseObserver.onError(Status.ALREADY_EXISTS.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DeadlockException) {
            val message = "Failed to create entity '${request.definition.entity.fqn()}': Deadlock with another transaction."
            LOGGER.info(message)
            responseObserver.onError(Status.ABORTED.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DatabaseException) {
            val message = "Failed to create entity '${request.definition.entity.fqn()}' because of a database error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            transaction.rollback()
        } catch (e: Throwable) {
            val message = "Failed to create entity '${request.definition.entity.fqn()}' because of an unknown error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
            transaction.rollback()
        }
    }


    /**
     * gRPC endpoint for dropping a specific [org.vitrivr.cottontail.database.entity.Entity].
     */
    override fun dropEntity(request: CottontailGrpc.DropEntityMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) {
        val queryId = UUID.randomUUID().toString()

        /* Obtain transaction or create new one. */
        val commitDirectly = !request.hasTxId()
        val transaction = if (commitDirectly) {
            this.manager.Transaction()
        } else {
            this.resumeTransaction(request.txId, responseObserver) ?: return
        }

        try {
            val entityName = request.entity.fqn()
            LOGGER.info("Dropping entity '$entityName'...")

            /* Execution operation. */
            val op = SpoolerSinkOperator(DropEntityOperator(this.catalogue, entityName), queryId, 0, responseObserver)
            transaction.execute(op)

            /* Finalize transaction. */
            if (commitDirectly && transaction.state == TransactionStatus.OPEN) {
                transaction.commit()
                LOGGER.info("Entity '$entityName' dropped successfully!")
            }  else {
                LOGGER.info("Entity '$entityName' dropped successfully (pending commit)!")
            }
            responseObserver.onCompleted()
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            val message = "Failed to drop entity '${request.entity.fqn()}': Schema does not exist."
            LOGGER.info(message)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DatabaseException.EntityDoesNotExistException) {
            val message = "Failed to drop entity '${request.entity.fqn()}': Entity does not exist."
            LOGGER.info(message)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DeadlockException) {
            val message = "Failed to drop entity '${request.entity.fqn()}': Deadlock with another transaction."
            LOGGER.info(message)
            responseObserver.onError(Status.ABORTED.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DatabaseException) {
            val message = "Failed to drop entity '${request.entity.fqn()}' because of a database error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            transaction.rollback()
        } catch (e: Throwable) {
            val message = "Failed to drop entity '${request.entity.fqn()}' because of an unknown error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
            transaction.rollback()
        }
    }

    /**
     * gRPC endpoint for truncating a specific [org.vitrivr.cottontail.database.entity.Entity].
     */
    override fun truncateEntity(request: CottontailGrpc.TruncateEntityMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) {
        val queryId = UUID.randomUUID().toString()

        /* Obtain transaction or create new one. */
        val commitDirectly = !request.hasTxId()
        val transaction = if (commitDirectly) {
            this.manager.Transaction()
        } else {
            this.resumeTransaction(request.txId, responseObserver) ?: return
        }

        try {
            val entityName = request.entity.fqn()
            LOGGER.info("Truncating entity '$entityName'...", entityName)

            /* Execution operation. */
            val op = SpoolerSinkOperator(TruncateEntityOperator(this.catalogue, entityName), queryId, 0, responseObserver)
            transaction.execute(op)

            /* Finalize transaction. */
            if (commitDirectly && transaction.state == TransactionStatus.OPEN) {
                transaction.commit()
                LOGGER.info("Entity '$entityName' truncated successfully!")
            }  else {
                LOGGER.info("Entity '$entityName' truncated successfully (pending commit)!")
            }
            responseObserver.onCompleted()
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            val message = "Failed to truncate entity '${request.entity.fqn()}': Schema does not exist."
            LOGGER.info(message)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DatabaseException.EntityDoesNotExistException) {
            val message = "Failed to truncate entity '${request.entity.fqn()}': Entity does not exist."
            LOGGER.info(message)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DeadlockException) {
            val message = "Failed to truncate entity '${request.entity.fqn()}': Deadlock with another transaction."
            LOGGER.info(message)
            responseObserver.onError(Status.ABORTED.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DatabaseException) {
            val message = "Failed to truncate entity '${request.entity.fqn()}' because of a database error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            transaction.rollback()
        } catch (e: Throwable) {
            val message = "Failed to truncate entity '${request.entity.fqn()}' because of an unknown error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
            transaction.rollback()
        }
    }

    /**
     * gRPC endpoint for optimizing a particular entity. Currently just rebuilds all the indexes.
     */
    override fun optimizeEntity(request: CottontailGrpc.OptimizeEntityMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) {
        val queryId = UUID.randomUUID().toString()

        /* Obtain transaction or create new one. */
        val commitDirectly = !request.hasTxId()
        val transaction = if (commitDirectly) {
            this.manager.Transaction()
        } else {
            this.resumeTransaction(request.txId, responseObserver) ?: return
        }

        try {
            val entityName = request.entity.fqn()
            LOGGER.info("Optimizing entity '$entityName'...")

            /* ToDo. */

            /* Update indexes. */
            //this.catalogue.schemaForName(entityName.schema()).entityForName(entityName).updateAllIndexes()

            /* Finalize transaction. */
            if (commitDirectly && transaction.state == TransactionStatus.OPEN) {
                transaction.commit()
                LOGGER.info("Entity '$entityName' optimized successfully!")
            }  else {
                LOGGER.info("Entity '$entityName' optimized successfully (pending commit)!")
            }
            responseObserver.onCompleted()
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            val message = "Failed to optimize entity '${request.entity.fqn()}': Schema does not exist."
            LOGGER.info(message)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DatabaseException.EntityDoesNotExistException) {
            val message = "Failed to optimize entity '${request.entity.fqn()}': Entity does not exist."
            LOGGER.info(message)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DeadlockException) {
            val message = "Failed to optimize entity '${request.entity.fqn()}': Deadlock with another transaction."
            LOGGER.info(message)
            responseObserver.onError(Status.ABORTED.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DatabaseException) {
            val message = "Failed to truncate entity '${request.entity.fqn()}' because of a database error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            transaction.rollback()
        } catch (e: Throwable) {
            val message = "Failed to truncate entity '${request.entity.fqn()}' because of an unknown error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
            transaction.rollback()
        }
    }

    /**
     * gRPC endpoint listing the available [org.vitrivr.cottontail.database.entity.Entity]s for the provided [org.vitrivr.cottontail.database.schema.Schema].
     */
    override fun listEntities(request: CottontailGrpc.ListEntityMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) {
        val queryId = UUID.randomUUID().toString()

        /* Obtain transaction or create new one. */
        val commitDirectly = !request.hasTxId()
        val transaction = if (commitDirectly) {
            this.manager.Transaction()
        } else {
            this.resumeTransaction(request.txId, responseObserver) ?: return
        }

        try {
            /* Extract schema name. */
            val schemaName = if (request.hasSchema()) {
                request.schema.fqn()
            } else {
                null
            }

            /* Execution operation. */
            val op = SpoolerSinkOperator(ListEntityOperator(this.catalogue, schemaName), queryId, 0, responseObserver)
            transaction.execute(op)

            /* Finalize transaction. */
            if (commitDirectly && transaction.state == TransactionStatus.OPEN) {
                transaction.commit()
            }
            responseObserver.onCompleted()
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            val message = "Failed to list entities for schema '${request.schema.fqn()}': Schema does not exist."
            LOGGER.info(message)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DeadlockException) {
            val message = "Failed to list entities for schema '${request.schema.fqn()}': Deadlock with another transaction."
            LOGGER.info(message)
            responseObserver.onError(Status.ABORTED.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DatabaseException) {
            val message = "Failed to list entities for schema '${request.schema.fqn()}' because of a database error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            transaction.rollback()
        } catch (e: Throwable) {
            val message = "Failed to list entities for schema '${request.schema.fqn()}' because of an unknown error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
            transaction.rollback()
        }
    }

    /**
     * gRPC endpoint for creating a particular [org.vitrivr.cottontail.database.index.Index]
     */
    override fun createIndex(request: CottontailGrpc.CreateIndexMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) {
        val queryId = UUID.randomUUID().toString()
        val resume = request.hasTxId()
        val transaction = if (resume) {
            this.resumeTransaction(request.txId, responseObserver) ?: return
        } else {
            this.manager.Transaction()
        }

        try {
            val indexName = request.definition.index.name.fqn()
            LOGGER.info("Creating index '$indexName'...")
            val columns = request.definition.columnsList.map {
                indexName.entity().column(it)
            }.toTypedArray()

            /* ToDo. */

            /* Notify caller of success. */
            LOGGER.info("Index '$indexName' created successfully!", request)
            responseObserver.onCompleted()
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            val message = "Failed to create index '${request.definition.index.name.fqn()}': Schema does not exist."
            LOGGER.info(message)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
        } catch (e: DatabaseException.EntityDoesNotExistException) {
            val message = "Failed to create index '${request.definition.index.name.fqn()}': Entity does not exist."
            LOGGER.info(message)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
        } catch (e: DatabaseException.ColumnDoesNotExistException) {
            val message = "Failed to create index '${request.definition.index.name.fqn()}': Column does not exist."
            LOGGER.error(message, e)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
        } catch (e: DatabaseException.IndexAlreadyExistsException) {
            val message = "Failed to create index '${request.definition.index.name.fqn()}': Index with identical name does already exist."
            LOGGER.error(message, e)
            responseObserver.onError(Status.ALREADY_EXISTS.withDescription(message).asException())
        } catch (e: DatabaseException) {
            val message = "Failed to create index '${request.definition.index.name.fqn()}' because of a database error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
        } catch (e: Throwable) {
            val message = "Failed to create index '${request.definition.index.name.fqn()}' because of an unknown error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
        }
    }

    /**
     * gRPC endpoint for dropping a particular [org.vitrivr.cottontail.database.index.Index]
     */
    override fun dropIndex(request: CottontailGrpc.DropIndexMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) {
        val queryId = UUID.randomUUID().toString()
        val resume = request.hasTxId()
        val transaction = if (resume) {
            this.resumeTransaction(request.txId, responseObserver) ?: return
        } else {
            this.manager.Transaction()
        }

        try {
            val indexName = request.index.fqn()
            LOGGER.info("Dropping index '$indexName'...")

            /* ToDo. */

            /* Notify caller of success. */
            responseObserver.onCompleted()
            LOGGER.info("Index '$indexName' dropped successfully!")
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            val message = "Failed to drop index '${request.index.fqn()}': Schema does not exist."
            LOGGER.info(message)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
        } catch (e: DatabaseException.EntityDoesNotExistException) {
            val message = "Failed to drop index '${request.index.fqn()}': Entity does not exist."
            LOGGER.info(message)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
        } catch (e: DatabaseException.IndexDoesNotExistException) {
            val message = "Failed to drop index '${request.index.fqn()}': Index does not exist."
            LOGGER.error(message, e)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
        } catch (e: DatabaseException) {
            val message = "Failed to drop index '${request.index.fqn()}' because of a database error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
        } catch (e: Throwable) {
            val message = "Failed to drop index '${request.index.fqn()}' because of an unknown error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
        }
    }

    /**
     * gRPC endpoint for rebuilding a particular [org.vitrivr.cottontail.database.index.Index]
     */
    override fun rebuildIndex(request: CottontailGrpc.RebuildIndexMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) {
        val queryId = UUID.randomUUID().toString()
        val resume = request.hasTxId()
        val transaction = if (resume) {
            this.resumeTransaction(request.txId, responseObserver) ?: return
        } else {
            this.manager.Transaction()
        }

        try {
            val indexName = request.index.fqn()
            LOGGER.info("Rebuilding index '$indexName'...")

            /* Update index. */
            /* ToDo. */

            /* Notify caller of success. */
            responseObserver.onCompleted()
            LOGGER.info("Index '$indexName' rebuilt successfully!")
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            val message = "Failed to rebuild index '${request.index.fqn()}': Schema does not exist."
            LOGGER.info(message)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
        } catch (e: DatabaseException.EntityDoesNotExistException) {
            val message = "Failed to rebuild index '${request.index.fqn()}': Entity does not exist."
            LOGGER.info(message)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
        } catch (e: DatabaseException.IndexDoesNotExistException) {
            val message = "Failed to rebuild index '${request.index.fqn()}': Index does not exist."
            LOGGER.error(message, e)
            responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
        } catch (e: DatabaseException) {
            val message = "Failed to rebuild index '${request.index.fqn()}' because of a database error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
        } catch (e: Throwable) {
            val message = "Failed to rebuild index '${request.index.fqn()}' because of an unknown error."
            LOGGER.error(message, e)
            responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
        }
    }
}