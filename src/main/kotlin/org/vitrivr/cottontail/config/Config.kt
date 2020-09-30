package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.utilities.serializers.PathSerializer
import java.nio.file.Path

/**
 * Cottntail DB configuration class.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
@Serializable
data class Config(
        /** Path to the root data folder used by Cottontail DB. */
        @Serializable(with = PathSerializer::class)
        val root: Path,

        /** Flag indicating whether to start the CLI upon starting Cottontail DB.*/
        val cli: Boolean = true,

        /** Default timeout for obtaining a file lock. */
        val lockTimeout: Long = 1000L,

        /** Reference to [ServerConfig], which contains configuration regarding the gRPC server. */
        val serverConfig: ServerConfig = ServerConfig(),

        /** Reference to [MemoryConfig], which contains configuration regarding the memory usage of Cottontail DB. */
        val memoryConfig: MemoryConfig = MemoryConfig(),

        /** Reference to [ExecutionConfig], which contains configuration regarding query execution in Cottontail DB. */
        val executionConfig: ExecutionConfig = ExecutionConfig()
)