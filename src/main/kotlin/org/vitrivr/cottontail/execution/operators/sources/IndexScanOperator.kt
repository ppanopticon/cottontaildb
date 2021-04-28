package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [AbstractEntityOperator] that scans an [Index] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class IndexScanOperator(groupId: GroupId, private val index: IndexTx, private val predicate: Predicate, private val partitionIndex: Int = 0, private val partitions: Int = 1) : Operator.SourceOperator(groupId) {

    /** The [ColumnDef] produced by this [IndexScanOperator]. */
    override val columns: Array<ColumnDef<*>>
        get() = this.index.dbo.produces

    /**
     * Converts this [IndexScanOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution.
     * @return [Flow] representing this [IndexScanOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        return if (this.partitions == 1) {
            flow {
                this@IndexScanOperator.index.filter(this@IndexScanOperator.predicate).forEach {
                    emit(it)
                }
            }
        } else {
            flow {
                this@IndexScanOperator.index.filterRange(this@IndexScanOperator.predicate, this@IndexScanOperator.partitionIndex, this@IndexScanOperator.partitions).forEach {
                    emit(it)
                }
            }
        }
    }
}