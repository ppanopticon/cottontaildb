package ch.unibas.dmi.dbis.cottontail.storage.engine.hare

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnType

interface HareColumnMetadata {
    /** The version of this [HareColumnFile]. */
    val version: Short

    /* The [ColumnType] held by this [HareColumnFile]. */
    val type: ColumnType<*>

    /** Whether entries in the [HareColumnFile] are nullable. */
    val nullable: Boolean

    /** Number of (un-deleted) rows in a [HareColumnFile]. */
    val rows: Long

    /** Number of deleted rows in a [HareColumnFile]. */
    val deleted: Long

    /** Number of (un-deleted) rows in a [HareColumnFile]. */
    val stripes: Int

    /** Size of a single column in bytes (including the metadata). */
    val columnSize: Int

    /** Size of a single stripe in bytes (including the metadata). */
    val stripeSize: Int
}