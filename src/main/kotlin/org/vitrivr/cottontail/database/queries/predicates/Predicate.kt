package org.vitrivr.cottontail.database.queries.predicates

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.Node
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A general purpose [Predicate] us can be used in a Cottontail DB query. Generally, [Predicate]s
 * are assumed to operate on [Record]s and usually affect a set of [ColumnDef]s in that [Record].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
interface Predicate : Node {
    /** An estimation of the CPU [Cost] required to apply this [Predicate] to a single [Record]. */
    val atomicCpuCost: Float

    /** Set of [ColumnDef] that are affected by this [Predicate]. */
    val columns: Set<ColumnDef<*>>

    /**
     * Performs value binding using the given [BindingContext]. Value binding is the act of
     * replacing a [Binding], which is a placeholder for a something, by the intended content.
     * This is an in-place operation!
     *
     * Used for caching and re-use of query plans.
     *
     * @param ctx [BindingContext] to use to resolve this [Binding].
     * @return This [Predicate].
     */
    override fun bindValues(ctx: BindingContext<Value>): Predicate
}





