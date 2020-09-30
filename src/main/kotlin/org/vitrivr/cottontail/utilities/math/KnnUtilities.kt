package org.vitrivr.cottontail.utilities.math

import org.vitrivr.cottontail.database.column.DoubleColumnType
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import org.vitrivr.cottontail.math.knn.selection.Selection
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.DoubleValue

/**
 * Utilities and constants used for nearest neighbor search.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
object KnnUtilities {

    /** Name of the distance column produced by a kNN [ExecutionTask]. */
    const val DISTANCE_COLUMN_NAME = "distance"

    /** Type of the distance column produced by a kNN [ExecutionTask]. */
    val DISTANCE_COLUMN_TYPE = DoubleColumnType

    /**
     * Transforms a [Selection] data structure into a [Recordset].
     *
     * @param columns The [ColumnDef] of the new [Recordset]
     * @param selection The [Selection] to convert.
     * @return [Recordset]
     */
    fun selectionToRecordset(columns: Array<ColumnDef<*>>, selection: Selection<ComparablePair<Record, DoubleValue>>): Recordset {
        val dataset = Recordset(columns, capacity = selection.size.toLong())
        for (i in 0 until selection.size) {
            dataset.addRow(selection[i].first.tupleId, arrayOf(*selection[i].first.values, DoubleValue(selection[i].second)))
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
        for (knn in list) {
            for (i in 0 until knn.size) {
                dataset.addRow(knn[i].first.tupleId, arrayOf(*knn[i].first.values, DoubleValue(knn[i].second)))
            }
        }
        return dataset
    }
}