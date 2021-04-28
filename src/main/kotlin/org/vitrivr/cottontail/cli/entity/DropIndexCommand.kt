package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.output.TermUi
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import io.grpc.StatusException
import org.vitrivr.cottontail.database.queries.binding.extensions.proto
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to drop an index for a specified entity from Cottontail DB.
 *
 * @author Loris Sauter
 * @version 1.0.3
 */
@ExperimentalTime
class DropIndexCommand(private val ddlStub: DDLGrpc.DDLBlockingStub) : AbstractEntityCommand(
    name = "drop-index",
    help = "Drops the index on an entity. Usage: entity drop-index <schema>.<entity> <index>"
) {

    /** Name of the index to drop. */
    private val indexName: Name.IndexName by argument(
        name = "name",
        help = "The name of the index. Ideally from a previous list-indices call"
    ).convert { this@DropIndexCommand.entityName.index(it) }

    /** Flag that can be used to directly provide confirmation. */
    private val confirm: Boolean by option(
        "-c",
        "--confirm",
        help = "Directly provides the confirmation option."
    ).flag()

    override fun exec() {
        if (this.confirm || TermUi.confirm(
                "Do you really want to drop the index ${this.indexName} [y/N]?",
                default = false,
                showDefault = false
            ) == true
        ) {
            try {
                val timedTable = measureTimedValue {
                    TabulationUtilities.tabulate(
                        this.ddlStub.dropIndex(
                            CottontailGrpc.DropIndexMessage.newBuilder()
                                .setIndex(this.indexName.proto())
                                .build()
                        )
                    )
                }
                println("Successfully dropped index ${this.indexName} (in ${timedTable.duration}).")
                print(timedTable.value)
            } catch (e: StatusException) {
                println("Failed to remove the index ${this.indexName} due to error: ${e.message}.")
            }
        } else {
            println("Drop index aborted.")
        }
    }
}