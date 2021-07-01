package org.vitrivr.cottontail.server.grpc

import io.grpc.ServerBuilder
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.server.grpc.services.DDLService
import org.vitrivr.cottontail.server.grpc.services.DMLService
import org.vitrivr.cottontail.server.grpc.services.DQLService
import org.vitrivr.cottontail.server.grpc.services.TXNService
import java.util.concurrent.ThreadPoolExecutor
import kotlin.time.ExperimentalTime

/**
 * Main server class for the gRPC endpoint provided by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
@ExperimentalTime
class CottontailGrpcServer(val config: Config, val catalogue: DefaultCatalogue) {

    /** The [ThreadPoolExecutor] used for handling gRPC calls and executing queries. */
    private val executor = this.config.execution.newExecutor()

    /** The [TransactionManager] used by this [CottontailGrpcServer] instance. */
    private val transactionManager: TransactionManager = TransactionManager(this.config.execution.transactionTableSize, this.config.execution.transactionHistorySize)

    /** Reference to the gRPC server. */
    private val server = ServerBuilder.forPort(this.config.server.port)
        .executor(this.executor)
        .addService(DDLService(this.catalogue, this.transactionManager))
        .addService(DMLService(this.catalogue, this.transactionManager))
        .addService(DQLService(this.catalogue, this.transactionManager))
        .addService(TXNService(this.transactionManager))
        .let {
            if (this.config.server.useTls) {
                val certFile = this.config.server.certFile!!.toFile()
                val privateKeyFile = this.config.server.privateKey!!.toFile()
                it.useTransportSecurity(certFile, privateKeyFile)
            } else {
                it
            }
        }.build()

    /**
     * Returns true if this [CottontailGrpcServer] is currently running, and false otherwise.
     */
    val isRunning: Boolean
        get() = !this.server.isShutdown

    /**
     * Starts this instance of [CottontailGrpcServer].
     */
    fun start() {
        this.server.start()
    }

    /**
     * Stops this instance of [CottontailGrpcServer].
     */
    fun stop() {
        this.server.shutdown()
        this.executor.shutdown()
    }
}