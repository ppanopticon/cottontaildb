package org.vitrivr.cottontail.cli.schema

import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.server.grpc.helper.proto
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to list available entities and schemata
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
class ListEntitiesCommand(private val ddlStub: DDLGrpc.DDLBlockingStub) : AbstractSchemaCommand(name = "list", help = "Lists all entities for a given schema. schema list <name>") {
    override fun exec() {
        /* Execute query. */
        val timedTable = measureTimedValue {
            TabulationUtilities.tabulate(this.ddlStub.listEntities(CottontailGrpc.ListEntityMessage.newBuilder().setSchema(this.schemaName.proto()).build()))
        }

        /* Output results. */
        println("${timedTable.value.rowCount} entities found for schema ${this.schemaName} (took ${timedTable.duration}).")
        print(timedTable.value)
    }
}