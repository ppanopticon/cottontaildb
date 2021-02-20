package org.vitrivr.cottontail.execution.operators.sort

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Record

/**
 * An abstract [Operator.PipelineOperator] used during query execution. Performs sorting on the specified [ColumnDef]s and  returns the [Record] in sorted order.
 *
 * Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractSortOperator(parent: Operator, sortOn: Array<ColumnDef<*>>, order: SortOrder) : Operator.PipelineOperator(parent) {

    /** The [AbstractSortOperator] retains the [ColumnDef] of the input. */
    override val columns: Array<ColumnDef<*>> = this.parent.columns

    /** The [AbstractSortOperator] is always a pipeline breaker. */
    override val breaker: Boolean = true

    /** The [Comparator] used for sorting. */
    protected val comparator: Comparator<Record> = order.wrap(when {
        sortOn.size == 1 && sortOn.first().nullable -> RecordComparator.SingleNullColumnComparator(sortOn.first())
        sortOn.size == 1 && !sortOn.first().nullable -> RecordComparator.SingleNonNullColumnComparator(sortOn.first())
        sortOn.size > 1 && !sortOn.any { it.nullable } -> RecordComparator.MultiNullColumnComparator(sortOn)
        else -> RecordComparator.MultiNonNullColumnComparator(sortOn)
    })
}