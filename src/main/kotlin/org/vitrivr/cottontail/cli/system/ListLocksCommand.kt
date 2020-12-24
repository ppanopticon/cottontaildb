package org.vitrivr.cottontail.cli.system

import com.google.protobuf.Empty
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.grpc.TXNGrpc
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * List all known locks on  Cottontail DB resources
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
class ListLocksCommand(private val txnStub: TXNGrpc.TXNBlockingStub) : AbstractCottontailCommand(name = "locks", help = "Lists all locks in the current Cottontail DB instance.") {
    override fun exec() {
        /* Execute query. */
        val timedTable = measureTimedValue {
            TabulationUtilities.tabulate(this@ListLocksCommand.txnStub.listLocks(Empty.getDefaultInstance()))
        }

        /* Output results. */
        println("${timedTable.value.rowCount} locks found (took ${timedTable.duration}).")
        print(timedTable.value)
    }
}