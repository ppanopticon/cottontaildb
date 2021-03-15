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
    fields: List<Pair<Name.ColumnName, Name.ColumnName?>>
) : Operator.PipelineOperator(parent) {

    /** True if names should be flattened, i.e., prefixes should be removed. */
    private val flattenNames = fields.all { it.first.schema() == fields.first().first.schema() }

    /** Columns produced by [SelectProjectionOperator]. */
    override val columns: Array<ColumnDef<*>> = this.parent.columns.mapNotNull { c ->
        val match = fields.find { f -> f.first.matches(c.name) }
        if (match != null) {
            val alias = match.second
            when {
                alias != null -> c.copy(name = alias)
                this.flattenNames -> c.copy(name = Name.ColumnName(c.name.simple))
                else -> c
            }
        } else {
            null
        }
    }.toTypedArray()


    /** Parent [ColumnDef] to access and aggregate. */
    private val parentColumns = this.parent.columns.filter { c ->
        fields.any { f -> f.first.matches(c.name) }
    }

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
            StandaloneRecord(r.tupleId, this.columns, this.parentColumns.map { r[it] }.toTypedArray())
        }
    }
}