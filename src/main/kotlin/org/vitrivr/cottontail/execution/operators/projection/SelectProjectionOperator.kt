package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.ColumnDef
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
 * @version 1.1.1
 */
class SelectProjectionOperator(parent: Operator, val fields: List<Pair<Name.ColumnName, Name.ColumnName?>>) : Operator.PipelineOperator(parent) {

    /** True if names should be flattened, i.e., prefixes should be removed. */
    private val flattenNames = this.fields.all { it.first.schema() == this.fields.first().first.schema() }

    /** Mapping from input [ColumnDef] to output [Name.ColumnName]. */
    private val mapping: Map<ColumnDef<*>, Name.ColumnName> = this.parent.columns.mapNotNull { c ->
        val name = this.fields.find { f -> f.first.matches(c.name) }
        if (name != null) {
            if (name.first.wildcard) {
                if (this.flattenNames) {
                    c to Name.ColumnName(c.name.simple)
                } else {
                    c to (c.name)
                }
            } else {
                if (this.flattenNames) {
                    c to (name.second ?: Name.ColumnName(name.first.simple))
                } else {
                    c to (name.second ?: name.first)
                }
            }
        } else {
            null
        }
    }.toMap()

    /** Columns produced by [SelectProjectionOperator]. */
    override val columns: Array<ColumnDef<*>> = this.mapping.entries.map {
        ColumnDef.withAttributes(it.value, it.key.type.name, it.key.logicalSize, it.key.nullable)
    }.toTypedArray()

    /** [MinProjectionOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [SelectProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [SelectProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        return this.parent.toFlow(context).map { r ->
            StandaloneRecord(r.tupleId, this.columns, this.mapping.map { r[it.key] }.toTypedArray())
        }
    }
}