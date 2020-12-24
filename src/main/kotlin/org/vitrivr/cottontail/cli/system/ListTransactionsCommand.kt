package org.vitrivr.cottontail.cli.system

import com.google.protobuf.Empty
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.grpc.TXNGrpc
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * List all known transactions in Cottontail DB.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
class ListTransactionsCommand(private val txnStub: TXNGrpc.TXNBlockingStub) : AbstractCottontailCommand(name = "transactions", help = "Lists all ongoing transaction in the current Cottontail DB instance.") {
    override fun exec() {
        /* Execute query. */
        val timedTable = measureTimedValue {
            TabulationUtilities.tabulate(this@ListTransactionsCommand.txnStub.listTransactions(Empty.getDefaultInstance()))
        }

        /* Output results. */
        println("${timedTable.value.rowCount} transactions found (took ${timedTable.duration}).")
        print(timedTable.value)
    }
}