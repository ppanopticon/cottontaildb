package org.vitrivr.cottontail.legacy

import org.vitrivr.cottontail.config.Config

/**
 * A [MigrationManager] can be used to migrate  Cottontail DB from older to newer versions.
 *
 * The backbone for data migration are legacy implementations of [Catalogue], [Schema], [Entity],
 * [Index] and [Column]. These legacy versions can be used to open old files and transfer data into
 * a fresh instance of Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface MigrationManager : AutoCloseable {
    /** The version this [MigrationManager] migrates from. */
    val from: Short

    /**
     * Executes the actual data migration.
     *
     * @param config The [Config] used to open the source database.
     */
    fun migrate(config: Config)
}