package org.vitrivr.cottontail.execution.operators.system

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

import org.vitrivr.cottontail.database.locking.LockManager
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An [Operator.SourceOperator] used during query execution. Used to list all locks.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ListLocksOperator(val manager: LockManager) : Operator.SourceOperator() {
    override val columns: Array<ColumnDef<*>>
        get() = arrayOf(
                ColumnDef.withAttributes(Name.ColumnName("dbo"), "STRING", -1, false),
                ColumnDef.withAttributes(Name.ColumnName("mode"), "STRING", -1, false),
                ColumnDef.withAttributes(Name.ColumnName("owner_count"), "INT", -1, false),
                ColumnDef.withAttributes(Name.ColumnName("owners"), "STRING", -1, false)
        )

    override fun toFlow(context: TransactionContext): Flow<Record> {
        return flow {
            var row = 0L
            val values = Array<Value?>(this@ListLocksOperator.columns.size) { null }
            this@ListLocksOperator.manager.allLocks().forEach { lock ->
                values[0] = StringValue(lock.first.toString())
                values[1] = StringValue(lock.second.getMode().toString())
                val owners = lock.second.getOwners().map { it.txId }
                values[2] = IntValue(owners.size)
                values[3] = StringValue(owners.joinToString(", "))
                emit(StandaloneRecord(row++, this@ListLocksOperator.columns, values))
            }
        }
    }
}