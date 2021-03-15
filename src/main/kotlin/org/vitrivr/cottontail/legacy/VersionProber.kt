package org.vitrivr.cottontail.legacy

import org.mapdb.CottontailStoreWAL
import org.mapdb.DB
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.catalogue.CatalogueHeader
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.legacy.v1.catalogue.CatalogueV1
import org.vitrivr.cottontail.legacy.v1.catalogue.CatalogueV1Header
import java.nio.file.Path

/**
 * A helper class that can be used to probe a Cottontail DB instance's [DBOVersion].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class VersionProber(private val config: Config) {

    companion object {
        val EXPECTED = DBOVersion.V2_0
    }

    /**
     * Probes the given Cottontail DB instance and returns its [DBOVersion].
     *
     * @param path The [Path] to the Cottontail DB data folder.
     * @return [DBOVersion] or [DBOVersion.UNDEFINED], if the path does not contain a valid Cottontail DB catalogue.
     */
    fun probe(path: Path): DBOVersion = when {
        isV2(path) -> DBOVersion.V2_0
        isV1(path) -> DBOVersion.V1_0
        else -> DBOVersion.UNDEFINED
    }

    /**
     * Checks if the database under the given [Path] is a [DBOVersion.V1_0] database.
     *
     * @param path The [Path] to check.
     * @return True if database is [DBOVersion.V1_0], false otherwise.
     */
    private fun isV1(path: Path): Boolean {
        /* Required because old store instances may still be around. This is due to how MapDB handled failure when creating a store. */
        System.gc()

        /* The [StoreWAL] that contains the Cottontail DB catalogue. */
        val store: CottontailStoreWAL? = try {
            this.config.mapdb.store(path.resolve(CatalogueV1.FILE_CATALOGUE))
        } catch (e: Throwable) {
            return false
        }

        val header = try {
            store?.get(CatalogueV1.HEADER_RECORD_ID, CatalogueV1Header.Serializer)
        } catch (e: Throwable) {
            store?.close()
            return false
        }

        /* Close store. */
        store?.close()
        return header != null
    }

    /**
     * Checks if the database under the given [Path] is a [DBOVersion.V2_0] database.
     *
     * @param path The [Path] to check.
     * @return True if database is [DBOVersion.V2_0], false otherwise.
     */
    private fun isV2(path: Path): Boolean {
        /* Required because old store instances may still be around. This is due to how MapDB handled failure when creating a store. */
        System.gc()

        /* The [StoreWAL] that contains the Cottontail DB catalogue. */
        val store: DB? = try {
            this.config.mapdb.db(path.resolve(DefaultCatalogue.FILE_CATALOGUE))
        } catch (e: Throwable) {
            return false
        }

        val header = try {
            store?.atomicVar(DefaultCatalogue.CATALOGUE_HEADER_FIELD, CatalogueHeader.Serializer)?.open()?.get()
        } catch (e: Throwable) {
            store?.close()
            return false
        }

        /* Close store. */
        store?.close()
        return header != null
    }
}