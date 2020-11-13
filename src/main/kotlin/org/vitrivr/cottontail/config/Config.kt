package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.utilities.serializers.PathSerializer
import java.nio.file.Path

/**
 * Cottontail DB configuration class.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
@Serializable
data class Config(
        /** Path to the root data folder used by Cottontail DB. */
        @Serializable(with = PathSerializer::class)
        val root: Path,

        /** Flag indicating whether to start the CLI upon starting Cottontail DB.*/
        val cli: Boolean = true,

        /** Reference to [ServerConfig], which contains configuration regarding the gRPC server. */
        val server: ServerConfig = ServerConfig(),

        /** Reference to [MapDBConfig], which contains configuration regarding the use of Map DB. */
        val mapdb: MapDBConfig = MapDBConfig(),

        /** Reference to [HareConfig], which contains configuration regarding the use of the HARE column format. */
        val hare: HareConfig = HareConfig(),

        /** Reference to [ExecutionConfig], which contains configuration regarding query execution in Cottontail DB. */
        val execution: ExecutionConfig = ExecutionConfig()
)