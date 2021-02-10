package org.vitrivr.cottontail.execution.operators.system

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An [Operator.SourceOperator] used during query execution. Used to list all ongoing transactions.
 *
 * @author Ralph Gasser
 * @version 1.0.2
 */
class ListTransactionsOperator(val manager: TransactionManager) : Operator.SourceOperator() {

    companion object {
        val COLUMNS: Array<ColumnDef<*>> = arrayOf(
            ColumnDef(Name.ColumnName("txId"), Type.Long, false),
            ColumnDef(Name.ColumnName("type"), Type.String, false),
            ColumnDef(Name.ColumnName("state"), Type.String, false),
            ColumnDef(Name.ColumnName("lock_count"), Type.Int, false),
            ColumnDef(Name.ColumnName("tx_count"), Type.Int, false),
            ColumnDef(Name.ColumnName("created"), Type.Date, false),
            ColumnDef(Name.ColumnName("ended"), Type.Date, false),
            ColumnDef(Name.ColumnName("duration[s]"), Type.Double, false)

        )
    }

    override val columns: Array<ColumnDef<*>> = COLUMNS

    override fun toFlow(context: TransactionContext): Flow<Record> {
        return flow {
            var row = 0L
            val values = Array<Value?>(this@ListTransactionsOperator.columns.size) { null }
            this@ListTransactionsOperator.manager.transactionHistory.forEach {
                values[0] = LongValue(it.txId)
                values[1] = StringValue(it.type.toString())
                values[2] = StringValue(it.state.toString())
                values[3] = IntValue(it.numberOfLocks)
                values[4] = IntValue(it.numberOfTxs)
                values[5] = DateValue(it.created)
                values[6] = if (it.ended != null) {
                    DateValue(it.ended!!)
                } else {
                    null
                }
                values[7] = if (it.ended != null) {
                    DoubleValue((it.ended!! - it.created) / 1000.0)
                } else {
                    DoubleValue((System.currentTimeMillis() - it.created) / 1000.0)

                }
                emit(StandaloneRecord(row++, this@ListTransactionsOperator.columns, values))
            }
        }
    }
}