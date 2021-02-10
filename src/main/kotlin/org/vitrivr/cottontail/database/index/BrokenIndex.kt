package org.vitrivr.cottontail.database.index

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import java.nio.file.Path

/**
 * This [Index] implementation signifies a broken [Index]. Indexes can break, e.g., due to structural
 * changes over different versions of Cottontail DB or due to data corruption.
 *
 * Since secondary [Index] structures are not required for the correct functioning of Cottontail DB,
 * broken indexes are mere placeholders for [Index]es that should exist, but don't work anymore and
 * are an alternative to crash the database upon startup.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class BrokenIndex(
    override val name: Name.IndexName,
    override val parent: Entity,
    override val type: IndexType,
    override val path: Path
) : Index() {
    override val columns: Array<ColumnDef<*>> = emptyArray()
    override val produces: Array<ColumnDef<*>> = emptyArray()
    override val supportsIncrementalUpdate: Boolean = false
    override val supportsPartitioning: Boolean = false
    override val dirty: Boolean = false
    override fun canProcess(predicate: Predicate): Boolean = false
    override fun cost(predicate: Predicate): Cost = Cost.INVALID
    override fun newTx(context: TransactionContext): IndexTx {
        throw UnsupportedOperationException("Broken index ${this.name} cannot be used to initiate a transaction! Drop and re-create the index.")
    }

    override val closed: Boolean = true
    override fun close() { /* No Op. */
    }
}