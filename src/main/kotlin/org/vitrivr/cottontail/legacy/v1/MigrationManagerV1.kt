package org.vitrivr.cottontail.legacy.v1

import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.legacy.AbstractMigrationManager
import org.vitrivr.cottontail.legacy.v1.catalogue.CatalogueV1
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.time.ExperimentalTime

/**
 * A [MigrationManager] to migrate Cottontail DB legacy version 1 database systems.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@ExperimentalTime
class MigrationManagerV1(batchSize: Int = 1_000_000, logFile: Path = Paths.get(".")) : AbstractMigrationManager(batchSize, logFile) {

    /** The version this [MigrationManagerV1] migrates from. */
    override val from: Short = 1

    /**
     * Tries to open the source [Catalogue] for migration.
     *
     * @param config The [Config] to open the [Catalogue] with.
     * @return Source [Catalogue] of null upon failure.
     */
    override fun openSourceCatalogue(config: Config): Catalogue? = try {
        CatalogueV1(config)
    } catch (e: Throwable) {
        null
    }

    /**
     * Tries to open the destination [Catalogue] for migration.
     *
     * @param config The [Config] to open the [Catalogue] with.
     * @return Source [Catalogue] of null upon failure.
     */
    override fun openDestinationCatalogue(config: Config): Catalogue? = try {
        DefaultCatalogue(config)
    } catch (e: Throwable) {
        null
    }
}