package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.arguments.argument
import io.grpc.StatusException
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.server.grpc.helper.proto
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to drop an index for a specified entity from Cottontail DB.
 *
 * @author Loris Sauter
 * @version 1.0.1
 */
@ExperimentalTime
class DropIndexCommand(private val ddlStub: DDLGrpc.DDLBlockingStub) : AbstractEntityCommand(name = "drop-index", help = "Drops the index on an entity. Usage: entity drop-index <schema>.<entity> <index>") {

    val name: String by argument(name = "name", help = "The name of the index. Ideally from a previous list-indices call")

    override fun exec() {
        val index = Name.IndexName(this.name)
        try {
            val timedTable = measureTimedValue {
                TabulationUtilities.tabulate(this.ddlStub.dropIndex(CottontailGrpc.DropIndexMessage.newBuilder().setIndex(index.proto()).build()))
            }
            println("Successfully dropped index ${this.name} (in ${timedTable.duration}).")
            print(timedTable.value)
        } catch (e: StatusException) {
            println("Failed to remove the index ${this.name} due to error: ${e.message}.")
        }
    }
}