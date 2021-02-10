package org.vitrivr.cottontail.execution.operators.system

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.Type
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.PhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates.FilterPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.LimitPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntityCountPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntityScanPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.IndexScanPhysicalNodeExpression
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.BooleanValue
import org.vitrivr.cottontail.model.values.FloatValue
import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An [Operator.SourceOperator] used during query execution. Used to explain queries
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ExplainQueryOperator(val candidates: Collection<PhysicalNodeExpression>) : Operator.SourceOperator() {

    companion object {
        val COLUMNS: Array<ColumnDef<*>> = arrayOf(
            ColumnDef(Name.ColumnName("path"), Type.String, false),
            ColumnDef(Name.ColumnName("name"), Type.String, false),
            ColumnDef(Name.ColumnName("output_size"), Type.Long, false),
            ColumnDef(Name.ColumnName("cost_cpu"), Type.Float, false),
            ColumnDef(Name.ColumnName("cost_io"), Type.Float, false),
            ColumnDef(Name.ColumnName("cost_memory"), Type.Float, false),
            ColumnDef(Name.ColumnName("partitionable"), Type.Boolean, false),
            ColumnDef(Name.ColumnName("comment"), Type.String, false)
        )
    }

    override val columns: Array<ColumnDef<*>> = COLUMNS

    override fun toFlow(context: TransactionContext): Flow<Record> {
        val candidate = this.candidates.sortedBy { it.totalCost }.first()
        return flow {
            val plan = enumerate(nodes = listOf(candidate))
            var row = 0L
            val array = Array<Value?>(this@ExplainQueryOperator.columns.size) { null }

            for (p in plan) {
                val node = p.second
                array[0] = StringValue(p.first)
                array[1] = StringValue(node.javaClass.simpleName)
                array[2] = LongValue(node.outputSize)
                array[3] = FloatValue(node.cost.cpu)
                array[4] = FloatValue(node.cost.io)
                array[5] = FloatValue(node.cost.memory)
                array[6] = BooleanValue(node.canBePartitioned)
                array[7] = when (node) {
                    is EntityCountPhysicalNodeExpression -> StringValue("${node.entity.name}")
                    is EntityScanPhysicalNodeExpression -> StringValue("${node.entity.name}")
                    is IndexScanPhysicalNodeExpression -> StringValue("${node.index.name}")
                    is FilterPhysicalNodeExpression -> StringValue("${node.predicate}")
                    is LimitPhysicalNodeExpression -> StringValue("SKIP ${node.skip} LIMIT ${node.limit}")
                    else -> null
                }
                emit(StandaloneRecord(row++, this@ExplainQueryOperator.columns, array))
            }
        }
    }


    /**
     * Enumerates a query plan and returns a sorted list of [PhysicalNodeExpression] with their exact path.
     */
    fun enumerate(path: Array<Int> = emptyArray(), nodes: List<PhysicalNodeExpression>): List<Pair<String, PhysicalNodeExpression>> {
        val list = mutableListOf<Pair<String, PhysicalNodeExpression>>()
        for ((index, node) in nodes.withIndex()) {
            val newPath = (path + index)
            if (node.inputs.size > 0) {
                list += this.enumerate(newPath, node.inputs as List<PhysicalNodeExpression>)
            }
            list.add(Pair(newPath.joinToString("."), node))
        }
        return list
    }
}