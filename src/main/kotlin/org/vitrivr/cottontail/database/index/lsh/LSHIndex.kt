package org.vitrivr.cottontail.database.index.lsh

import org.mapdb.DB
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.extensions.write
import org.vitrivr.cottontail.utilities.math.KnnUtilities
import java.nio.file.Path

abstract class LSHIndex<T : VectorValue<*>>(
    final override val name: Name.IndexName,
    final override val parent: Entity,
    final override val columns: Array<ColumnDef<*>>,
    override val path: Path,
    params: Map<String, String>? = null
) : Index() {

    /** Index-wide constants. */
    companion object {
        const val MAP_FIELD_NAME = "lsh_map"
    }

    /** The [LSHIndex] implementation returns exactly the columns that is indexed. */
    final override val produces: Array<ColumnDef<*>> =
        arrayOf(KnnUtilities.queryIndexColumnDef(this.name.entity()))

    /** The type of [Index] */
    override val type: IndexType = IndexType.LSH

    /** The internal [DB] reference. */
    protected val db: DB = this.parent.parent.parent.config.mapdb.db(this.path)

    /** Flag indicating if this [LSHIndex] has been closed. */
    @Volatile
    final override var closed: Boolean = false
        private set

    /**
     * Closes this [SuperBitLSHIndex] and the associated data structures.
     */
    final override fun close() = this.closeLock.write {
        if (!this.closed) {
            this.db.close()
            this.closed = true
        }
    }
}