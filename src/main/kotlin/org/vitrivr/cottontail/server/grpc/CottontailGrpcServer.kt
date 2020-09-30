package org.vitrivr.cottontail.server.grpc

import io.grpc.ServerBuilder
import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import org.vitrivr.cottontail.config.ServerConfig
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.server.grpc.services.CottonDDLService
import org.vitrivr.cottontail.server.grpc.services.CottonDMLService
import org.vitrivr.cottontail.server.grpc.services.CottonDQLService
import java.util.concurrent.ExecutorService
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.time.ExperimentalTime

/**
 * Main server class for the gRPC endpoint provided by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.2
 */
@ExperimentalTime
class CottontailGrpcServer(val config: ServerConfig, val catalogue: Catalogue, private val engine: ExecutionEngine) {

    /** The [ThreadPoolExecutor] used for handling the individual GRPC calls. */
    private val executor: ExecutorService = ThreadPoolExecutor(this.config.coreThreads, this.config.maxThreads, this.config.keepAliveTime, TimeUnit.MILLISECONDS, SynchronousQueue())

    /** Reference to the gRPC server. */
    private val server = ServerBuilder.forPort(config.port)
            .executor(this.executor)
            .addService(CottonDDLService(this.catalogue))
            .addService(CottonDMLService(this.catalogue, this.engine))
            .addService(CottonDQLService(this.catalogue, this.engine))
            .let {
                if (config.useTls) {
                    val certFile = config.certFile?.toFile() ?: throw Exception()
                    val privateKeyFile = config.privateKey?.toFile() ?: throw Exception()
                    it.useTransportSecurity(certFile, privateKeyFile)
                } else {
                    it
                }
            }.build()


    /** Companion object with Logger reference. */
    companion object {
        val LOGGER: Logger = LogManager.getLogger(CottontailGrpcServer::class.qualifiedName)
    }

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
        LOGGER.info("Cottontail DB server is up and running at port ${config.port} ! Hop along...")
    }

    /**
     * Stops this instance of [CottontailGrpcServer].
     */
    fun stop() {
        this.server.shutdown()
        LOGGER.info("Cottontail DB was shut down. Have a binky day!")
    }
}