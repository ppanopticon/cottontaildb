package ch.unibas.dmi.dbis.cottontail.database.index.lsh

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnType
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.index.IndexType
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.values.types.VectorValue
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name

import org.mapdb.DBMaker
import org.mapdb.HTreeMap
import org.mapdb.Serializer

import java.nio.file.Path

abstract class LSHIndex<T : VectorValue<*>>(final override val name: Name, final override val parent: Entity, final override val columns: Array<ColumnDef<*>>, params: Map<String, String>? = null) : Index() {
    /** Index-wide constants. */
    companion object {
        const val MAP_FIELD_NAME = "lsh_map"
    }

    /** Constant FQN of the [Schema] object. */
    final override val fqn: Name = this.parent.fqn.append(this.name)

    /** Path to the [SuperBitLSHIndex] file. */
    final override val path: Path = this.parent.path.resolve("idx_lsh_$name.db")

    /** The [SuperBitLSHIndex] implementation returns exactly the columns that is indexed. */
    final override val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef(parent.fqn.append("distance"), ColumnType.forName("DOUBLE")))

    /** The type of [Index] */
    final override val type: IndexType = IndexType.LSH

    /** The internal [DB] reference. */
    protected val db = if (parent.parent.parent.config.forceUnmapMappedFiles) {
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