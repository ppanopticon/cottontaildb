package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.utilities.serializers.PathSerializer
import java.nio.file.Path

/**
 * Config for Cottontail DB's gRPC server.
 *
 *
 * @param port The port under which Cottontail DB demon should be listening for calls. Defaults to 1865
 * @param keepAliveTime The number of milliseconds to wait before decommissioning unused threads.
 * @param certFile Path to the certificate file used for TLS.
 * @param privateKey Path to the private key used for TLS.
 *
 * @author Ralph Gasser
 * @version 1.0.2
 */
@Serializable
data class ServerConfig(
    /** The port under which the server should be reachable. */
    val port: Int = 1865,

    /** Keep-alive time for request. */
    val keepAliveTime: Long = 500,

    /** Optional path to certificate file (for SSL). */
    @Serializable(with = PathSerializer::class) val certFile: Path? = null,

    /** Optional path to private key file (fpr SSL). */
    @Serializable(with = PathSerializer::class) val privateKey: Path? = null
) {

    /**
     * True if TLS should be used for gRPC communication, false otherwise.
     */
    val useTls
        get() = (this.certFile != null && this.privateKey != null)
}



