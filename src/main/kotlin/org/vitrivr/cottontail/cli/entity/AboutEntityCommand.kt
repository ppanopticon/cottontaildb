package org.vitrivr.cottontail.cli.entity

import io.grpc.StatusException
import org.vitrivr.cottontail.database.queries.binding.extensions.proto
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command for accessing and reviewing [org.vitrivr.cottontail.database.entity.DefaultEntity] details.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
class AboutEntityCommand(private val dqlStub: DDLGrpc.DDLBlockingStub) : AbstractEntityCommand(name = "about", help = "Gives an overview of the entity and its columns.") {
    override fun exec() {
        try {
            val timedTable = measureTimedValue {
                TabulationUtilities.tabulate(this.dqlStub.entityDetails(CottontailGrpc.EntityDetailsMessage.newBuilder().setEntity(this.entityName.proto()).build()))
            }
            println("Details for entity ${this.entityName} (took ${timedTable.duration}):")
            print(timedTable.value)
        } catch (e: StatusException) {
            println("Failed to load details for entity ${this.entityName}: ${e.message}.")
        }
    }
}