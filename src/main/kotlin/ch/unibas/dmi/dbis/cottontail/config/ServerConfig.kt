package ch.unibas.dmi.dbis.cottontail.config

import ch.unibas.dmi.dbis.cottontail.server.ServerEnum
import ch.unibas.dmi.dbis.cottontail.utilities.serializers.PathSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import java.nio.file.Path

/**
 * Config for Cottontail DB's internal server.
 *
 * @param port The port under which Cottontail DB demon should be listening for calls. Defaults to 1865
 * @param messageSize The the maximum size an incoming gRPC message can have. Defaults to 4'194'304 bytes (4096 kbytes).
 * @param coreThreads The core size of the thread pool used to run the gRPC server.
 * @param maxThreads The maximum number of threads that handle calls to the gRPC server.
 * @param keepAliveTime The number of milliseconds to wait before decommissioning unused threads.
 * @param certFile Path to the certificate file used for TLS.
 * @param privateKey Path to the private key used for TLS.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
@Serializable
data class ServerConfig(
    val server: ServerEnum = ServerEnum.AVATICA,
    val port: Int = 1865,
    val messageSize: Int = 4194304,
    val coreThreads: Int = Runtime.getRuntime().availableProcessors() / 2,
    val maxThreads: Int = Runtime.getRuntime().availableProcessors() * 2,
    val keepAliveTime: Long = 500,
    @Serializable(with=PathSerializer::class) val certFile: Path? = null,
    @Serializable(with=PathSerializer::class) val privateKey: Path? = null) {

    /**
     * True if TLS should be used for gRPC communication, false otherwise.
     */
    @Transient val useTls
        get() = this.certFile != null && this.privateKey != null

}



