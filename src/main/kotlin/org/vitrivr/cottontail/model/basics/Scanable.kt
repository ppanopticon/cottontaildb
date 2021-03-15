package org.vitrivr.cottontail.model.basics

import org.vitrivr.cottontail.database.column.ColumnDef

/**
 * An objects that holds [Record] values and allows for scanning operation on those [Record] values.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
interface Scanable {
    /**
     * Returns an [Iterator] for all the [Record]s in this [Scanable].
     *
     * @param columns The [ColumnDef]s that should be scanned.
     * @return [Iterator]
     */
    fun scan(columns: Array<ColumnDef<*>>): Iterator<Record>

    /**
     * Returns an [Iterator] for all the [TupleId]s contained in the provide [LongRange]
     * and this [Scanable]. Can be used for partitioning.
     *
     * @param columns The [ColumnDef]s that should be scanned.
     * @param range The [LongRange] to iterate over
     * @return [Iterator]
     */
    fun scan(columns: Array<ColumnDef<*>>, range: LongRange): Iterator<Record>
}