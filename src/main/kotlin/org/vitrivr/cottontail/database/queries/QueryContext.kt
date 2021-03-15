package org.vitrivr.cottontail.database.queries

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.vitrivr.cottontail.database.column.ColumnDef

import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A context for query binding and planning. Tracks logical and physical query plans and
 * enables late binding of [Binding]s to [Node]s
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class QueryContext(val txn: TransactionContext) {

    /** List of bound [Value]s for this [QueryContext]. */
    val values = BindingContext<Value>()

    /** List of bound [Record]s for this [QueryContext]. */
    val records = BindingContext<Record>()

    /** The individual [OperatorNode.Logical], each representing different sub-queries. */
    private val nodes: MutableMap<GroupId, OperatorNode.Logical> = Int2ObjectOpenHashMap()

    /** The [OperatorNode.Logical] representing the query and the sub-queries held by this [QueryContext]. */
    val logical: OperatorNode.Logical?
        get() = this.nodes[0]

    /** The [OperatorNode.Physical] representing the query and the sub-queries held by this [QueryContext]. */
    var physical: OperatorNode.Physical? = null
        internal set

    /** Output [ColumnDef] for the query held by this [QueryContext] (as per canonical plan). */
    val output: Array<ColumnDef<*>>?
        get() = this.nodes[0]?.columns

    /** Output order for the query held by this [QueryContext] (as per canonical plan). */
    val order: Array<Pair<ColumnDef<*>, SortOrder>>?
        get() = this.nodes[0]?.order

    @Volatile
    private var groupIdCounter: GroupId = 0

    /**
     * Returns the next available [GroupId].
     *
     * @return
     */
    fun nextGroupId(): GroupId = this.groupIdCounter++

    /**
     * Registers an [OperatorNode.Logical] with this [QueryContext] and assigns a new [GroupId] for it.
     */
    fun register(plan: OperatorNode.Logical) {
        this.nodes[plan.groupId] = plan
    }

    /**
     * Returns the [OperatorNode.Logical] for the given [GroupId].
     *
     * @param groupId The [GroupId] to return an [OperatorNode.Logical] for.
     * @return [OperatorNode.Logical]
     */
    operator fun get(groupId: GroupId): OperatorNode.Logical = this.nodes[groupId] ?: throw QueryException.QueryPlannerException("Failed to access sub-query with groupId $groupId")

    /**
     * Returns an executable [Operator] for this [QueryContext]. Requires a functional, [OperatorNode.Physical]
     */
    fun toOperatorTree(txn: TransactionContext): Operator {
        val local = this.physical
        check(local != null) { IllegalStateException("Cannot generate an operator tree without a valid, physical node expression tree.") }
        return local.toOperator(txn, this)
    }
}