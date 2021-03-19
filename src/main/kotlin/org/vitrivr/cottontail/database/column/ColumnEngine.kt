package org.vitrivr.cottontail.database.column

import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.column.hare.HareColumn
import org.vitrivr.cottontail.database.column.mapdb.MapDBColumn
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.model.values.types.Value
import java.nio.file.Path

/**
 * The driver or engine of [Column], i.e., the type of data storage that is used underneath.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class ColumnEngine {
    /** A MapDB based column. */
    MAPDB,

    /** Cottontail DB's own storage format. Currently not supported and here for future support. */
    HARE;

    /**
     * Opens a [Column] of this [ColumnEngine].
     *
     * @param path The [Path] to the [Column] file.
     * @param parent The parent [DefaultEntity].
     */
    fun open(path: Path, parent: DefaultEntity): Column<Value> = when (this) {
        MAPDB -> MapDBColumn(path, parent)
        HARE -> HareColumn(path, parent)
    }

    /**
     * Creates a [Column] of this [ColumnEngine]
     *
     * @param path The location at which to create the [Column].
     * @param columnDef [ColumnDef] describing the [Column].
     * @param config The [Config] to use.
     */
    fun create(path: Path, columnDef: ColumnDef<*>, config: Config) = when (this) {
        MAPDB -> MapDBColumn.initialize(path, columnDef, config.mapdb)
        HARE -> HareColumn.initialize(path, columnDef, config.hare)
    }
}