package org.vitrivr.cottontail.model.basics

import org.vitrivr.cottontail.database.column.ColumnDef

/**
 * An objects that holds [Record] values and allows for scanning operation on those [Record] values.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.4.0
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
     * @param partitionIndex The [partitionIndex] for this [scan] call.
     * @param partitions The total number of partitions for this [scan] call.
     * @return [Iterator]
     */
    fun scan(columns: Array<ColumnDef<*>>, partitionIndex: Int, partitions: Int): Iterator<Record>
}