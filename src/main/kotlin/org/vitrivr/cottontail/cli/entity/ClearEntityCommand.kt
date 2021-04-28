package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.output.TermUi
import org.vitrivr.cottontail.database.queries.binding.extensions.protoFrom
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DMLGrpc
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to clear [org.vitrivr.cottontail.database.entity.DefaultEntity], i.e., delete all data from it
 *
 * @author Ralph Gasser
 * @version 1.0
 */
@ExperimentalTime
class ClearEntityCommand(private val dmlStub: DMLGrpc.DMLBlockingStub) : AbstractEntityCommand(
    name = "clear",
    help = "Clears the given entity, i.e., deletes all data it contains. Usage: entity clear <schema>.<entity>"
) {

    override fun exec() {
        if (TermUi.confirm(
                "Do you really want to delete all data from entity ${this.entityName} (y/N)?",
                default = false,
                showDefault = false
            ) == true
        ) {
            val time = measureTimedValue {
                this.dmlStub.delete(
                    CottontailGrpc.DeleteMessage.newBuilder().setFrom(this.entityName.protoFrom()).build()
                )
            }
            println("Successfully cleared entity ${this.entityName} (took ${time.duration}).")
            print(TabulationUtilities.tabulate(time.value))
        } else {
            println("Clear entity aborted.")
        }
    }
}