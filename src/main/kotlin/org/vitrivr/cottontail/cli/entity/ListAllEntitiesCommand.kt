package org.vitrivr.cottontail.cli.entity


import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * List all [org.vitrivr.cottontail.database.entity.DefaultEntity] stored in Cottontail DB.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.2
 */
@ExperimentalTime
class ListAllEntitiesCommand(private val ddlStub: DDLGrpc.DDLBlockingStub) : AbstractCottontailCommand(name = "all", help = "Lists all entities stored in Cottontail DB.") {
    override fun exec() {
        /* Execute query. */
        val timedTable = measureTimedValue {
            TabulationUtilities.tabulate(this.ddlStub.listEntities(CottontailGrpc.ListEntityMessage.newBuilder().build()))
        }

        /* Output results. */
        println("${timedTable.value.rowCount - 1} entities found (took ${timedTable.duration}).")
        print(timedTable.value)
    }
}