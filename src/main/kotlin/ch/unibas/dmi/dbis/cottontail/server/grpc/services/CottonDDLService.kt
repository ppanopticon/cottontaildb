package ch.unibas.dmi.dbis.cottontail.server.grpc.services

import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnType
import ch.unibas.dmi.dbis.cottontail.database.index.IndexType
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDDLGrpc
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import ch.unibas.dmi.dbis.cottontail.server.grpc.helper.fqn
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import ch.unibas.dmi.dbis.cottontail.utilities.name.NameType

import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory

/**
 * This is a GRPC service endpoint that handles DDL (=Data Definition Language) request for Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class CottonDDLService (val catalogue: Catalogue): CottonDDLGrpc.CottonDDLImplBase() {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(CottonDDLService::class.java)
    }
    /**
     * gRPC endpoint for creating a new [Schema][ch.unibas.dmi.dbis.cottontail.database.schema.Schema]
     */
    override fun createSchema(request: CottontailGrpc.Schema, responseObserver: StreamObserver<CottontailGrpc.SuccessStatus>) = try {
        LOGGER.trace("Creating schema {}", request.name)
        val name = Name(request.name)
        if (name.type != NameType.SIMPLE) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to create schema: Invalid name '${request.name}'.").asException())
        } else {
            this.catalogue.createSchema(name)
            responseObserver.onNext(CottontailGrpc.SuccessStatus.newBuilder().setTimestamp(System.currentTimeMillis()).build())
            responseObserver.onCompleted()
        }
    } catch (e: DatabaseException.SchemaAlreadyExistsException) {
        LOGGER.error("Error while creating schema", e)
        responseObserver.onError(Status.ALREADY_EXISTS.withDescription("Schema '${request.name}' cannot be created because it already exists!").asException())
    } catch (e: DatabaseException) {
        LOGGER.error("Error while creating schema", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to create schema '${request.name}' because of database error: ${e.message}").asException())
    } catch (e: Throwable) {
        LOGGER.error("Error while creating schema", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to create schema '${request.name}' 'because unknown error: ${e.message}").asException())
    }

    /**
     * gRPC endpoint for dropping a [Schema][ch.unibas.dmi.dbis.cottontail.database.schema.Schema]
     */
    override fun dropSchema(request: CottontailGrpc.Schema, responseObserver: StreamObserver<CottontailGrpc.SuccessStatus>) = try {
        LOGGER.trace("Dropping schema {}", request.name)
        val schemaName = Name(request.name)
        if (schemaName.type != NameType.SIMPLE) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to drop schema: Invalid name '${request.name}'.").asException())
        } else {
            this.catalogue.dropSchema(schemaName)
            responseObserver.onNext(CottontailGrpc.SuccessStatus.newBuilder().setTimestamp(System.currentTimeMillis()).build())
            responseObserver.onCompleted()
        }
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        LOGGER.error("Error while dropping schema '${request.name}'", e)
        responseObserver.onError(Status.NOT_FOUND.withDescription("Schema '${request.name}' does not exist!").asException())
    } catch (e: DatabaseException) {
        LOGGER.error("Error while dropping schema '${request.name}'", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to drop schema '${request.name}' because of database error: ${e.message}").asException())
    } catch (e: Throwable) {
        LOGGER.error("Error while dropping schema '${request.name}'", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to drop schema '${request.name}' because unknown error: ${e.message}").asException())
    }

    /**
     * gRPC endpoint listing the available [Schema][ch.unibas.dmi.dbis.cottontail.database.schema.Schema]s.
     */
    override fun listSchemas(request: CottontailGrpc.Empty, responseObserver: StreamObserver<CottontailGrpc.Schema>) = try {
        this.catalogue.schemas.forEach {
            responseObserver.onNext(CottontailGrpc.Schema.newBuilder().setName(it.name).build())
        }
        responseObserver.onCompleted()
    } catch (e: DatabaseException) {
        LOGGER.error("Error while listing schemas", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to list schemas because of database error: ${e.message}").asException())
    } catch (e: Throwable) {
        LOGGER.error("Error while listing schemas", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to list schemas because of unknown error: ${e.message}").asException())
    }

    /**
     *
     * gRPC endpoint for creating a new [Entity][ch.unibas.dmi.dbis.cottontail.database.entity.Entity]
     */
    override fun createEntity(request: CottontailGrpc.CreateEntityMessage, responseObserver: StreamObserver<CottontailGrpc.SuccessStatus>) = try {
        LOGGER.trace("Creating entity {}", request.entity.name)
        val entityName = Name(request.entity.name)
        val schemaName = Name(request.entity.schema.name)
        if (entityName.type != NameType.SIMPLE) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to create entity: Invalid entity name '${request.entity.name}'.").asException())
        } else if (schemaName.type != NameType.SIMPLE) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to create entity: Invalid schema name '${request.entity.schema.name}'.").asException())
        } else {
            val schema = this.catalogue.schemaForName(schemaName)
            val columns = request.columnsList.map {
                val type = ColumnType.forName(it.type.name)
                ColumnDef(Name(it.name), type, it.length, it.nullable)
            }
            schema.createEntity(entityName, *columns.toTypedArray())
            responseObserver.onNext(CottontailGrpc.SuccessStatus.newBuilder().setTimestamp(System.currentTimeMillis()).build())
            responseObserver.onCompleted()
        }
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        LOGGER.error("Error while creating entity '${request.entity.fqn()}'", e)
        responseObserver.onError(Status.NOT_FOUND.withDescription("Schema '${request.entity.schema.name} does not exist!").asException())
    } catch (e: DatabaseException.EntityAlreadyExistsException) {
        LOGGER.error("Error while creating entity '${request.entity.fqn()}'", e)
        responseObserver.onError(Status.ALREADY_EXISTS.withDescription("Entity '${request.entity.fqn()} does already exist!").asException())
    } catch (e: DatabaseException) {
        LOGGER.error("Error while creating entity '${request.entity.fqn()}'", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to create entity '${request.entity.fqn()}' because of database error: ${e.message}").asException())
    } catch (e: Throwable) {
        LOGGER.error("Error while creating entity '${request.entity.fqn()}'", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to create entity '${request.entity.fqn()}' because of unknown error: ${e.message}").asException())
    }

    /**
     * gRPC endpoint for dropping a particular [Schema][ch.unibas.dmi.dbis.cottontail.database.schema.Schema]
     */
    override fun dropEntity(request: CottontailGrpc.Entity, responseObserver: StreamObserver<CottontailGrpc.SuccessStatus>) = try {
        val entityName = Name(request.name)
        val schemaName = Name(request.schema.name)
        if (entityName.type != NameType.SIMPLE) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to drop entity: Invalid entity name '${request.name}'.").asException())
        } else if (schemaName.type != NameType.SIMPLE) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to drop entity: Invalid schema name '${request.schema.name}'.").asException())
        } else {
            this.catalogue.schemaForName(schemaName).dropEntity(entityName)
            responseObserver.onNext(CottontailGrpc.SuccessStatus.newBuilder().setTimestamp(System.currentTimeMillis()).build())
            responseObserver.onCompleted()
        }
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        LOGGER.error("Error while dropping entity '${request.fqn()}'", e)
        responseObserver.onError(Status.NOT_FOUND.withDescription("Schema '${request.schema.fqn()}' does not exist!").asException())
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        LOGGER.error("Error while dropping entity '${request.fqn()}'", e)
        responseObserver.onError(Status.NOT_FOUND.withDescription("Entity '${request.fqn()}' does not exist!").asException())
    } catch (e: DatabaseException) {
        LOGGER.error("Error while dropping entity '${request.fqn()}'", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to drop entity '${request.fqn()}' because of database error: ${e.message}").asException())
    } catch (e: Throwable) {
        LOGGER.error("Error while dropping entity '${request.fqn()}'", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to drop entity '${request.fqn()}' because of unknown error: ${e.message}").asException())
    }

    /**
     * gRPC endpoint listing the available [Entity][ch.unibas.dmi.dbis.cottontail.database.entity.Entity]s
     * for the provided [Schema][ch.unibas.dmi.dbis.cottontail.database.schema.Schema].
     */
    override fun listEntities(request: CottontailGrpc.Schema, responseObserver: StreamObserver<CottontailGrpc.Entity>) = try {
        val schemaName = Name(request.name)
        if (schemaName.type != NameType.SIMPLE) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to list entities: Invalid schema name '${request.name}'.").asException())
        }  else {
            val builder = CottontailGrpc.Entity.newBuilder()
            this.catalogue.schemaForName(schemaName).entities.forEach {
                responseObserver.onNext(builder.setName(it.name).setSchema(request).build())
            }
            responseObserver.onCompleted()
        }
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        LOGGER.error("Error while listing entities", e)
        responseObserver.onError(Status.NOT_FOUND.withDescription("Schema '${request.name} does not exist!").asException())
    } catch (e: DatabaseException) {
        LOGGER.error("Error while listing entities", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to list entities for schema ${request.name} because of database error: ${e.message}").asException())
    } catch (e: Throwable) {
        LOGGER.error("Error while listing entities", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to list entities for schema ${request.name} because of unknown error: ${e.message}").asException())
    }

    /**
     * gRPC endpoint for creating a particular [Index][ch.unibas.dmi.dbis.cottontail.database.index.Index]
     */
    override fun createIndex(request: CottontailGrpc.CreateIndexMessage, responseObserver: StreamObserver<CottontailGrpc.SuccessStatus>) = try {
        LOGGER.trace("Creating index {}", request)
        val indexName = Name(request.index.name)
        val entityName = Name(request.index.entity.name)
        val schemaName = Name(request.index.entity.schema.name)
        if (entityName.type != NameType.SIMPLE) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to create index: Invalid entity name '${request.index.entity.name}'.").asException())
        } else if (schemaName.type != NameType.SIMPLE) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to create index: Invalid schema name '${request.index.entity.schema.name}'.").asException())
        } else if (indexName.type != NameType.SIMPLE) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to create index: Invalid index name '${request.index.name}'.").asException())
        } else {
            val entity = this.catalogue.schemaForName(schemaName).entityForName(entityName)
            val columns = request.columnsList.map { entity.columnForName(Name(it)) ?: throw DatabaseException.ColumnDoesNotExistException(entity.fqn.append(it)) }.toTypedArray()

            /* Creates and updates the index. */
            entity.createIndex(indexName, IndexType.valueOf(request.index.type.toString()), columns, request.paramsMap)

            /* Notify caller of success. */
            responseObserver.onNext(CottontailGrpc.SuccessStatus.newBuilder().setTimestamp(System.currentTimeMillis()).build())
            responseObserver.onCompleted()
        }
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        LOGGER.error("Error while creating index '${request.index.fqn()}'", e)
        responseObserver.onError(Status.NOT_FOUND.withDescription("Schema '${request.index.entity.schema.fqn()} does not exist!").asException())
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        LOGGER.error("Error while creating index '${request.index.fqn()}'", e)
        responseObserver.onError(Status.NOT_FOUND.withDescription("Entity '${request.index.entity.fqn()} does not exist!").asException())
    } catch (e: DatabaseException.IndexAlreadyExistsException) {
        LOGGER.error("Error while creating index '${request.index.fqn()}'", e)
        responseObserver.onError(Status.ALREADY_EXISTS.withDescription("Index '${request.index.fqn()}' does already exist!").asException())
    } catch (e: DatabaseException.ColumnDoesNotExistException) {
        LOGGER.error("Error while creating index '${request.index.fqn()}'", e)
        responseObserver.onError(Status.NOT_FOUND.withDescription(e.message).asException())
    } catch (e: DatabaseException) {
        LOGGER.error("Error while creating index '${request.index.fqn()}'", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to create index '${request.index.fqn()}' because of database error: ${e.message}").asException())
    } catch (e: Throwable) {
        LOGGER.error("Error while creating index '${request.index.fqn()}'", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to create index '${request.index.fqn()}' because of an unknown error: ${e.message}").asException())
    }

    /**
     * gRPC endpoint for dropping a particular [Index][ch.unibas.dmi.dbis.cottontail.database.index.Index]
     */
    override fun dropIndex(request: CottontailGrpc.DropIndexMessage, responseObserver: StreamObserver<CottontailGrpc.SuccessStatus>) = try {
        val indexName = Name(request.index.name)
        val entityName = Name(request.index.entity.name)
        val schemaName = Name(request.index.entity.schema.name)
        if (entityName.type != NameType.SIMPLE) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to drop index: Invalid entity name '${request.index.entity.name}'.").asException())
        } else if (schemaName.type != NameType.SIMPLE) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to drop index: Invalid schema name '${request.index.entity.schema.name}'.").asException())
        } else if (indexName.type != NameType.SIMPLE) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to drop index: Invalid index name '${request.index.name}'.").asException())
        } else {
            this.catalogue.schemaForName(schemaName).entityForName(entityName).dropIndex(indexName)

            /* Notify caller of success. */
            responseObserver.onNext(CottontailGrpc.SuccessStatus.newBuilder().setTimestamp(System.currentTimeMillis()).build())
            responseObserver.onCompleted()
        }
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        LOGGER.error("Error while dropping index '${request.index.fqn()}'", e)
        responseObserver.onError(Status.NOT_FOUND.withDescription("Schema '${request.index.entity.schema.fqn()} does not exist!").asException())
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        LOGGER.error("Error while dropping index '${request.index.fqn()}'", e)
        responseObserver.onError(Status.NOT_FOUND.withDescription("Entity '${request.index.entity.fqn()} does not exist!").asException())
    } catch (e: DatabaseException.IndexDoesNotExistException) {
        LOGGER.error("Error while dropping index '${request.index.fqn()}'", e)
        responseObserver.onError(Status.NOT_FOUND.withDescription("Index '${request.index.fqn()} does not exist!").asException())
    } catch (e: DatabaseException) {
        LOGGER.error("Error while dropping index '${request.index.fqn()}'", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to drop index '${request.index.fqn()}' because of database error: ${e.message}").asException())
    } catch (e: Throwable) {
        LOGGER.error("Error while dropping index '${request.index.fqn()}'", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to drop index '${request.index.fqn()}' because of an unknown error: ${e.message}").asException())
    }

    /**
     * gRPC endpoint for rebuilding a particular [Index][ch.unibas.dmi.dbis.cottontail.database.index.Index]
     */
    override fun rebuildIndex(request: CottontailGrpc.RebuildIndexMessage, responseObserver: StreamObserver<CottontailGrpc.SuccessStatus>) = try {
        val indexName = Name(request.index.name)
        val entityName = Name(request.index.entity.name)
        val schemaName = Name(request.index.entity.schema.name)
        if (entityName.type != NameType.SIMPLE) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to rebuild index: Invalid entity name '${request.index.entity.name}'.").asException())
        } else if (schemaName.type != NameType.SIMPLE) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to rebuild index: Invalid schema name '${request.index.entity.schema.name}'.").asException())
        } else if (indexName.type != NameType.SIMPLE) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to rebuild index: Invalid index name '${request.index.name}'.").asException())
        } else {
            this.catalogue.schemaForName(schemaName).entityForName(entityName).updateIndex(indexName)

            /* Notify caller of success. */
            responseObserver.onNext(CottontailGrpc.SuccessStatus.newBuilder().setTimestamp(System.currentTimeMillis()).build())
            responseObserver.onCompleted()
        }
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        LOGGER.error("Error while rebuilding index '${request.index.fqn()}'", e)
        responseObserver.onError(Status.NOT_FOUND.withDescription("Schema '${request.index.entity.schema.fqn()} does not exist!").asException())
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        LOGGER.error("Error while rebuilding index '${request.index.fqn()}'", e)
        responseObserver.onError(Status.NOT_FOUND.withDescription("Entity '${request.index.entity.fqn()} does not exist!").asException())
    } catch (e: DatabaseException.IndexDoesNotExistException) {
        LOGGER.error("Error while rebuilding index '${request.index.fqn()}'", e)
        responseObserver.onError(Status.NOT_FOUND.withDescription("Index '${request.index.fqn()} does not exist!").asException())
    } catch (e: DatabaseException) {
        LOGGER.error("Error while rebuilding index '${request.index.fqn()}'", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to rebuild index '${request.index.fqn()}' because of database error: ${e.message}").asException())
    } catch (e: Throwable) {
        LOGGER.error("Error while rebuilding index '${request.index.fqn()}'", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to rebuild index '${request.index.fqn()}' because of an unknown error: ${e.message}").asException())
    }

    /**
     * gRPC endpoint for optimizing a particular entity. Currently just rebuilds all the indexes.
     */
    override fun optimizeEntity(request: CottontailGrpc.Entity, responseObserver: StreamObserver<CottontailGrpc.SuccessStatus>) = try {
        val entityName = Name(request.name)
        val schemaName = Name(request.schema.name)
        if (entityName.type != NameType.SIMPLE) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to optimize entity: Invalid entity name '${request.name}'.").asException())
        } else if (schemaName.type != NameType.SIMPLE) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to optimize entity: Invalid schema name '${request.schema.name}'.").asException())
        } else {
            this.catalogue.schemaForName(schemaName).entityForName(entityName).updateAllIndexes()

            /* Notify caller of success. */
            responseObserver.onNext(CottontailGrpc.SuccessStatus.newBuilder().setTimestamp(System.currentTimeMillis()).build())
            responseObserver.onCompleted()
        }
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        LOGGER.error("Error while optimizing entity '${request.fqn()}'", e)
        responseObserver.onError(Status.NOT_FOUND.withDescription("Schema '${request.schema.fqn()} does not exist!").asException())
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        LOGGER.error("Error while optimizing entity '${request.fqn()}'", e)
        responseObserver.onError(Status.NOT_FOUND.withDescription("Entity '${request.fqn()} does not exist!").asException())
    } catch (e: DatabaseException) {
        LOGGER.error("Error while optimizing entity '${request.fqn()}'", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to optimize entity '${request.fqn()}' because of database error: ${e.message}").asException())
    } catch (e: Throwable) {
        LOGGER.error("Error while optimizing entity '${request.fqn()}'", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Failed to optimize entity '${request.fqn()}' because of an unknown error: ${e.message}").asException())
    }
}
