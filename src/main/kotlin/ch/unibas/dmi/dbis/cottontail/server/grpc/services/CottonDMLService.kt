package ch.unibas.dmi.dbis.cottontail.server.grpc.services

import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.Transaction
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDMLGrpc
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.recordset.StandaloneRecord
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import ch.unibas.dmi.dbis.cottontail.model.exceptions.ValidationException
import ch.unibas.dmi.dbis.cottontail.model.type.*
import ch.unibas.dmi.dbis.cottontail.model.values.Value
import ch.unibas.dmi.dbis.cottontail.server.grpc.helper.*
import ch.unibas.dmi.dbis.cottontail.utilities.name.append
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

internal class CottonDMLService (val catalogue: Catalogue): CottonDMLGrpc.CottonDMLImplBase() {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(CottonDMLService::class.java)
    }

    /**
     * gRPC endpoint for inserting data in a batch mode. All the data provided with the [CottontailGrpc.InsertMessage] will be inserted
     * in a single transaction. I.e. either the insert succeeds or fails completely.
     */
    override fun insert(request: CottontailGrpc.InsertMessage, responseObserver: StreamObserver<CottontailGrpc.InsertStatus>) = try {
        val schema = this.catalogue.schemaForName(request.entity.schema.name)
        val entity = schema.entityForName(request.entity.name)
        entity.Tx(false).begin { tx ->
            request.tupleList.map { it.dataMap }.forEach { entry ->
                val columns = mutableListOf<ColumnDef<*>>()
                val values = mutableListOf<Value<*>?>()
                entry.map {
                    val col = entity.columnForName(it.key) ?: throw DatabaseException.ColumnDoesNotExistException(entity.fqn.append(it.key))
                    columns.add(col)
                    values.add(castToColumn(it.value, col))
                }
                tx.insert(StandaloneRecord(columns = columns.toTypedArray(), init = values.toTypedArray()))
            }
            true
        }
        responseObserver.onNext(CottontailGrpc.InsertStatus.newBuilder().setSuccess(true).setTimestamp(System.currentTimeMillis()).build())
        responseObserver.onCompleted()

        /* Log... */
        LOGGER.trace("Successfully persisted ${request.tupleList.size} tuples to '${request.entity.fqn()}' (with commit).")
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        responseObserver.onError(Status.NOT_FOUND.withDescription("Insert failed because schema '${request.entity.schema.name} does not exist!").asException())
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        responseObserver.onError(Status.NOT_FOUND.withDescription("Insert failed because entity '${request.entity.name} does not exist!").asException())
    } catch (e: DatabaseException.ColumnDoesNotExistException) {
        responseObserver.onError(Status.NOT_FOUND.withDescription("Insert failed because column '${e.column}' does not exist!").asException())
    } catch (e: ValidationException) {
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Insert failed because data validation failed: ${e.message}").asException())
    }  catch (e: DatabaseException) {
        responseObserver.onError(Status.INTERNAL.withDescription("Insert failed because of a database error: ${e.message}").asException())
    } catch (e: Throwable) {
        responseObserver.onError(Status.UNKNOWN.withDescription("Insert failed because of a unknown error: ${e.message}").asException())
    }

    /**
     * gRPC endpoint for inserting data in a streaming mode; transactions will stay open until the caller explicitly completes them
     * or until an error occurs. As new entities are being inserted, new transactions will be created and thus new lock will be acquired.
     */
    override fun insertStream(responseObserver: StreamObserver<CottontailGrpc.InsertStatus>): StreamObserver<CottontailGrpc.InsertMessage> = object:StreamObserver<CottontailGrpc.InsertMessage>{

        /** List of all the [Tx] associated with this call. */
        private val transactions = ConcurrentHashMap<String, Entity.Tx>()

        /** Flag indicating that call was closed. */
        @Volatile
        private var closed = false

        init {
            LOGGER.trace("Streaming INSERT transaction was initiated by client.")
        }

        /**
         * Called whenever a new [CottontailGrpc.InsertMessage] arrives.
         *
         * @param request The next [CottontailGrpc.InsertMessage] message.
         */
        override fun onNext(request: CottontailGrpc.InsertMessage) {
            try {
                /* Check if call was closed and return. */
                if (closed) return

                /* Extract required schema and entity. */
                val schema = this@CottonDMLService.catalogue.schemaForName(request.entity.schema.name)
                val entity = schema.entityForName(request.entity.name)

                /* Re-use or create Transaction. */
                var tx = this.transactions[request.entity.fqn()]
                if (tx == null) {
                    tx = entity.Tx(false)
                    this.transactions[request.entity.fqn()] = tx
                }

                /* Do the insert. */
                request.tupleList.map { it.dataMap }.forEach { entry ->
                    val columns = mutableListOf<ColumnDef<*>>()
                    val values = mutableListOf<Value<*>?>()
                    entry.map {
                        val col = entity.columnForName(it.key) ?: throw DatabaseException.ColumnDoesNotExistException(entity.fqn.append(it.key))
                        columns.add(col)
                        values.add(castToColumn(it.value, col))
                    }
                    tx.insert(StandaloneRecord(columns = columns.toTypedArray(), init = values.toTypedArray()))
                }

                /* Log... */
                LOGGER.trace("Successfully inserted ${request.tupleList.size} tuples into '${request.entity.fqn()}' (no commit).")

                /* Respond with status. */
                responseObserver.onNext(CottontailGrpc.InsertStatus.newBuilder().setSuccess(true).setTimestamp(System.currentTimeMillis()).build())
            } catch (e: DatabaseException.SchemaDoesNotExistException) {
                responseObserver.onError(Status.NOT_FOUND.withDescription("Insert failed because schema '${request.entity.schema.name} does not exist!").asException())
                this.cleanup()
            } catch (e: DatabaseException.EntityDoesNotExistException) {
                responseObserver.onError(Status.NOT_FOUND.withDescription("Insert failed because entity '${request.entity.fqn()} does not exist!").asException())
                this.cleanup()
            } catch (e: DatabaseException.ColumnDoesNotExistException) {
                responseObserver.onError(Status.NOT_FOUND.withDescription("Insert failed because column '${e.column}' does not exist!").asException())
                this.cleanup()
            } catch (e: ValidationException) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Insert failed because data validation failed: ${e.message}").asException())
                this.cleanup()
            }  catch (e: DatabaseException) {
                responseObserver.onError(Status.INTERNAL.withDescription("Insert failed because of a database error: ${e.message}").asException())
                this.cleanup()
            } catch (e: Throwable) {
                responseObserver.onError(Status.UNKNOWN.withDescription("Insert failed because of a unknown error: ${e.message}").asException())
                this.cleanup()
            }
        }

        /**
         * Called when the client-side indicates an error condition.
         */
        override fun onError(t: Throwable) {
            LOGGER.trace("Streaming INSERT transaction was aborted by client. Reason: ${t.message}")
            this.cleanup()
        }

        /**
         * Called when client-side completes invocation, effectively ending the [Transaction].
         */
        override fun onCompleted() {
            LOGGER.trace("Streaming INSERT transaction was committed by client.")
            this.cleanup(true)
        }

        /**
         * Commits or rolls back all the  [Transaction]s associated with this call and closes them afterwards.
         *
         * @param commit If true [Transaction]s are commited before closing. Otherwise, they are rolled back.
         */
        private fun cleanup(commit: Boolean = false) {
            this.closed = true
            this.transactions.forEach {
                try {
                    if (commit) {
                        it.value.commit()
                    } else {
                        it.value.rollback()
                    }
                } finally {
                    it.value.close()
                }
            }
        }
    }

    /**
     * Casts the provided [CottontailGrpc.Data] to a data type supported by the provided [ColumnDef]
     *
     * @param value The [CottontailGrpc.Data] to cast.
     * @param col The [ColumnDef] of the column, the data should be stored in.
     * @return The converted value.
     */
    private fun castToColumn(value: CottontailGrpc.Data, col: ColumnDef<*>) : Value<*>? = if (
            value.dataCase == CottontailGrpc.Data.DataCase.DATA_NOT_SET || value.dataCase == null
    ) {
        null
    } else {
        when (col.type) {
            is BooleanType -> value.toBooleanValue()
            is ByteType -> value.toByteValue()
            is ShortType -> value.toShortValue()
            is IntType -> value.toIntValue()
            is LongType -> value.toLongValue()
            is FloatType -> value.toFloatValue()
            is DoubleType -> value.toDoubleValue()
            is StringType -> value.toStringValue()
            is IntArrayType -> value.toIntVectorValue()
            is LongArrayType -> value.toLongVectorValue()
            is FloatArrayType -> value.toFloatVectorValue()
            is DoubleArrayType -> value.toDoubleVectorValue()
            is BooleanArrayType -> value.toBooleanVectorValue()
        }
    }
}