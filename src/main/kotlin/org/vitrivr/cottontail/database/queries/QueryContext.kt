package org.vitrivr.cottontail.database.queries

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.vitrivr.cottontail.database.queries.binding.ValueBinding
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.PhysicalOperatorNode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A context for query binding and planning. Tracks logical and physical query plans and
 * enables late binding of [Binding]s to [Node]s
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class QueryContext {

    /** List of bound [Value]s for this [QueryContext]. */
    private var bindings = Object2ObjectOpenHashMap<ValueBinding, Value?>()

    /** The size of this [QueryContext], i.e., the number of values bound. */
    val size: Int
        get() = this.bindings.size

    /** The [LogicalOperatorNode] representing the query held by this [QueryContext]. */
    var logical: LogicalOperatorNode? = null
        internal set

    /** The [PhysicalOperatorNode] representing the query held by this [QueryContext]. */
    var physical: PhysicalOperatorNode? = null
        internal set

    /**
     * Returns an executable [Operator] for this [QueryContext]. Requires a functional, [PhysicalOperatorNode]
     */
    fun toOperatorTree(txn: TransactionContext): Operator {
        val local = this.physical
        check(local != null) { IllegalStateException("Cannot generate an operator tree without a valid, physical node expression tree.") }
        return local.toOperator(txn, this)
    }

    /**
     * Returns the [Value] for the given [ValueBinding].
     *
     * @param binding The [ValueBinding] to lookup.
     * @return The bound [Value].
     */
    operator fun get(binding: ValueBinding): Value? {
        require(this.bindings.contains(binding)) { "Binding $binding is not known to this query context." }
        return this.bindings[binding]
    }

    /**
     * Binds a [Value] to this [QueryContext].
     *
     * @param value The [Value] to bind.
     * @return The [ValueBinding].
     */
    fun bind(value: Value): ValueBinding {
        val bound = ValueBinding(this.bindings.size, value.type)
        this.bindings[bound] = value
        return bound
    }

    /**
     * Binds a NULL [Value] to this [QueryContext].
     *
     * @param type The [Type] of the NULL value to bind.
     * @param type The logical size of the NULL value to bind.
     *
     * @return The [ValueBinding].
     */
    fun bindNull(type: Type<*>): ValueBinding {
        val bound = ValueBinding(this.bindings.size, type)
        this.bindings[bound] = null
        return bound
    }
}