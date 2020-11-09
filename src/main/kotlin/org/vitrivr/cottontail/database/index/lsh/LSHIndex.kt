package org.vitrivr.cottontail.database.index.lsh

import org.mapdb.DBMaker
import org.mapdb.HTreeMap
import org.mapdb.Serializer
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Path

abstract class LSHIndex<T : VectorValue<*>>(final override val name: Name.IndexName, final override val catalogue: Catalogue, final override val columns: Array<ColumnDef<*>>, params: Map<String, String>? = null) : Index() {
    /** Index-wide constants. */
    companion object {
        const val MAP_FIELD_NAME = "lsh_map"
    }

    /** Path to the [LSHIndex] file. */
    final override val path: Path = this.catalogue.indexForName(this.name).path

    /** The [LSHIndex] implementation returns exactly the columns that is indexed. */
    final override val produces: Array<ColumnDef<*>> = emptyArray()

    /** The type of [Index] */
    override val type: IndexType = IndexType.LSH

    /** The internal database reference. */
    protected val db = if (this.catalogue.config.memoryConfig.forceUnmapMappedFiles) {
        DBMaker.fileDB(this.path.toFile()).fileMmapEnable().cleanerHackEnable().transactionEnable().make()
    } else {
        DBMaker.fileDB(this.path.toFile()).fileMmapEnable().transactionEnable().make()
    }

    /** Map structure used for [LSHIndex]. Contains bucket ID and maps it to array of longs. */
    protected val map: HTreeMap<Int, LongArray> = this.db.hashMap(MAP_FIELD_NAME, Serializer.INTEGER, Serializer.LONG_ARRAY).counterEnable().createOrOpen()

    /** Flag indicating if this [LSHIndex] has been closed. */
    @Volatile
    final override var closed: Boolean = false
        private set

    /**
     * Closes this [SuperBitLSHIndex] and the associated data structures.
     */
    final override fun close() = this.globalLock.write {
        if (!this.closed) {
            this.db.close()
            this.closed = true
        }
    }
}