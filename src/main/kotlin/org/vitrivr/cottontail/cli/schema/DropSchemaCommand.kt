package org.vitrivr.cottontail.cli.schema

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.prompt
import org.vitrivr.cottontail.database.queries.binding.extensions.proto
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to drop a [org.vitrivr.cottontail.database.schema.DefaultSchema] from Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
class DropSchemaCommand(private val ddlStub: DDLGrpc.DDLBlockingStub) : AbstractSchemaCommand(name = "drop", help = "Drops the schema with the given name. Usage: schema drop <name>") {
    /** Flag that can be used to directly provide confirmation. */
    private val confirm: Boolean by option(
        "-c",
        "--confirm",
        help = "Directly provides the confirmation option."
    ).convert {
        it.toLowerCase() == "y"
    }.prompt(
        "Do you really want to drop the schema ${this.schemaName} [y/N]?",
        default = "n",
        showDefault = false
    )


    override fun exec() {
        if (this.confirm) {
            /* Execute query. */
            val timedTable = measureTimedValue {
                TabulationUtilities.tabulate(
                    this.ddlStub.dropSchema(
                        CottontailGrpc.DropSchemaMessage.newBuilder()
                            .setSchema(this.schemaName.proto()).build()
                    )
                )
            }

            /* Output results. */
            println("Schema ${this.schemaName} dropped successfully (took ${timedTable.duration}).")
            print(timedTable.value)
        } else {
            println("Drop schema aborted.")
        }
    }
}