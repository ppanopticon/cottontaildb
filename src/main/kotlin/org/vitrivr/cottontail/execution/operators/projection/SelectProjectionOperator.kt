package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord

/**
 * An [Operator.PipelineOperator] used during query execution. It generates new [Record]s for
 * each incoming [Record] and removes / renames field according to the [fields] definition provided.
 *
 * Only produces a single [Record].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class SelectProjectionOperator(
    parent: Operator,
    fields: List<Pair<ColumnDef<*>, Name.ColumnName?>>
) : Operator.PipelineOperator(parent) {

    /** True if names should be flattened, i.e., prefixes should be removed. */
    private val flattenNames =
        fields.all { it.first.name.schema() == fields.first().first.name.schema() }

    /** Columns produced by [SelectProjectionOperator]. */
    override val columns: Array<ColumnDef<*>> = fields.map {
        val alias = it.second
        if (alias != null) {
            it.first.copy(name = alias)
        } else if (flattenNames) {
            it.first.copy(name = Name.ColumnName(it.first.name.simple))
        } else {
            it.first
        }
    }.toTypedArray()

    /** Parent [ColumnDef] to access and aggregate. */
    private val parentColumns = fields.map { it.first }

    /** [SelectProjectionOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [SelectProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [SelectProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        return this.parent.toFlow(context).map { r ->
            StandaloneRecord(
                r.tupleId,
                this.columns,
                this.parentColumns.map { r[it] }.toTypedArray()
            )
        }
    }
}