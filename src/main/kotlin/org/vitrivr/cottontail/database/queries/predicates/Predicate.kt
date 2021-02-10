package org.vitrivr.cottontail.database.queries.predicates

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.Node
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.model.basics.Record

/**
 * A general purpose [Predicate] us can be used in a Cottontail DB query. Generally, [Predicate]s
 * are assumed to operate on [Record]s and usually affect a set of [ColumnDef]s in that [Record].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
interface Predicate : Node {
    /** An estimation of the CPU [Cost] required to apply this [Predicate] to a single [Record]. */
    val cost: Float

    /** Set of [ColumnDef] that are affected by this [Predicate]. */
    val columns: Set<ColumnDef<*>>
}





