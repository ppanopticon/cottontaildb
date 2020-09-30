package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.basics.OperatorStatus
import org.vitrivr.cottontail.execution.operators.basics.PipelineOperator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord

/**
 * An [PipelineOperator] used during query execution. It generates new [Record]s for each incoming
 * [Record] and removes / renames field according to the [fields] definition provided.
 *
 * Only produces a single [Record].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class SelectProjectionOperator(parent: Operator, context: ExecutionEngine.ExecutionContext, val fields: List<Pair<Name.ColumnName, Name.ColumnName?>>) : PipelineOperator(parent, context) {

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

    override fun prepareOpen() { /*NoOp */
    }

    override fun prepareClose() { /*NoOp */
    }

    /**
     * Converts this [SelectProjectionOperator] to a [Flow] and returns it.
     *
     * @param scope The [CoroutineScope] used for execution
     * @return [Flow] representing this [SelectProjectionOperator]
     * @throws IllegalStateException If this [Operator.status] is not [OperatorStatus.OPEN]
     */
    override fun toFlow(scope: CoroutineScope): Flow<Record> {
        check(this.status == OperatorStatus.OPEN) { "Cannot convert operator $this to flow because it is in state ${this.status}." }
        return this.parent.toFlow(scope).map { r ->
            StandaloneRecord(r.tupleId, this.columns, this.mapping.map { r[it.key] }.toTypedArray())
        }
    }
}