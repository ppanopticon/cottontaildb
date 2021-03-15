package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.utilities.serializers.PathSerializer
import java.nio.file.Path

/**
 * Config for Cottontail DB's gRPC server.
 *
 * @author Ralph Gasser
 * @version 1.0.3
 */
@Serializable
data class ServerConfig(
    /** The port under which the gRPC server should be reachable. */
    val port: Int = 1865,

    /** Optional path to certificate file (for TLS). */
    @Serializable(with = PathSerializer::class)
    val certFile: Path? = null,

    /** Optional path to private key file (for TLS). */
    @Serializable(with = PathSerializer::class)
    val privateKey: Path? = null
) {

    /**
     * True if TLS should be used for gRPC communication, false otherwise.
     */
    val useTls
        get() = (this.certFile != null && this.privateKey != null)
}



