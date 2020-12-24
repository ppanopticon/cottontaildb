package org.vitrivr.cottontail.utilities.math

import org.vitrivr.cottontail.database.column.DoubleColumnType
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import org.vitrivr.cottontail.math.knn.selection.Selection
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * Utilities and constants used for nearest neighbor search.
 *
 * @author Ralph Gasser
 * @version 1.1.1
 */
object KnnUtilities {

    /** Name of the distance column produced by a nearest neighbor search (NNS / kNN). */
    const val DISTANCE_COLUMN_NAME = "distance"

    /** Type of the distance column produced by a nearest neighbor search (NNS / kNN). */
    val DISTANCE_COLUMN_TYPE = DoubleColumnType

    /**
     * Generates and returns a [ColumnDef] for a distance column.
     *
     * @param name The [Name.EntityName] to generate the [ColumnDef] for.
     * @return [ColumnDef]
     */
    fun columnDef(name: Name.EntityName? = null) = ColumnDef(name?.column(DISTANCE_COLUMN_NAME)
            ?: Name.ColumnName(DISTANCE_COLUMN_NAME), DISTANCE_COLUMN_TYPE)

    /**
     * Transforms a [Selection] data structure into a [Recordset].
     *
     * @param columns The [ColumnDef] of the new [Recordset]
     * @param selection The [Selection] to convert.
     * @return [Recordset]
     */
    fun selectionToRecordset(columns: Array<ColumnDef<*>>, selection: Selection<ComparablePair<Record, DoubleValue>>): Recordset {
        val dataset = Recordset(columns, capacity = selection.size.toLong())
        val buffer = ArrayList<Value?>(columns.size)
        for (i in 0 until selection.size) {
            buffer.clear()
            selection[i].first.forEach { _, v -> buffer.add(v) }
            buffer.add(DoubleValue(selection[i].second))
            dataset.addRow(selection[i].first.tupleId, buffer.toTypedArray())
        }
        return dataset
    }

    /**
     * Combines a [Collection] of [Selection] data structures into a [Recordset].
     *
     * @param columns The [ColumnDef] of the new [Recordset]
     * @param list List of [MinHeapSelection]s to combine.
     * @return [Recordset]
     */
    fun selectionsToRecordset(columns: Array<ColumnDef<*>>, list: Collection<Selection<ComparablePair<Record, DoubleValue>>>): Recordset {
        val dataset = Recordset(columns, capacity = (list.size * list.first().size).toLong())
        val buffer = ArrayList<Value?>(columns.size)
        for (knn in list) {
            for (i in 0 until knn.size) {
                buffer.clear()
                knn[i].first.forEach { _, v -> buffer.add(v) }
                buffer.add(DoubleValue(knn[i].second))
                dataset.addRow(knn[i].first.tupleId, buffer.toTypedArray())
            }
        }
        return dataset
    }
}