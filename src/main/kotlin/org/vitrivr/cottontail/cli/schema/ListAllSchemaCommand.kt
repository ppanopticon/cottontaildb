package org.vitrivr.cottontail.cli.schema

import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * List all [org.vitrivr.cottontail.database.schema.DefaultSchema] stored in Cottontail DB.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
class ListAllSchemaCommand(private val ddlStub: DDLGrpc.DDLBlockingStub) : AbstractCottontailCommand(name = "all", help = "Lists all schemas stored in Cottontail DB. Usage: schema all") {
    override fun exec() {
        /* Execute query. */
        val timedTable = measureTimedValue {
            TabulationUtilities.tabulate(this@ListAllSchemaCommand.ddlStub.listSchemas(CottontailGrpc.ListSchemaMessage.getDefaultInstance()))
        }

        /* Output results. */
        println("${timedTable.value.rowCount} schemas found (took ${timedTable.duration}).")
        print(timedTable.value)
    }
}