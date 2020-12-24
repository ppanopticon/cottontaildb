package org.vitrivr.cottontail.cli.schema

import io.grpc.StatusException
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.server.grpc.helper.proto
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to create a [org.vitrivr.cottontail.database.schema.Schema] from Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
class CreateSchemaCommand(private val ddlStub: DDLGrpc.DDLBlockingStub) : AbstractSchemaCommand(name = "create", help = "Create the schema with the given name. Usage: schema create <name>") {
    override fun exec() {
        /* Execute query. */
        try {
            val timedTable = measureTimedValue {
                TabulationUtilities.tabulate(this.ddlStub.createSchema(CottontailGrpc.CreateSchemaMessage.newBuilder().setSchema(this.schemaName.proto()).build()))
            }

            /* Output results. */
            println("Schema ${this.schemaName} created successfully (took ${timedTable.duration}).")
            print(timedTable.value)
        } catch (e: StatusException) {
            println("Failed to to create ${this.schemaName}: ${e.message}.")
        }
    }
}