package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.prompt
import io.grpc.StatusException
import org.vitrivr.cottontail.database.queries.binding.extensions.proto
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to drop, i.e., remove a [org.vitrivr.cottontail.database.entity.DefaultEntity] by its name.
 *
 * @author Ralph Gasser
 * @version 1.0.3
 */
@ExperimentalTime
class DropEntityCommand(private val ddlStub: DDLGrpc.DDLBlockingStub) : AbstractEntityCommand(name = "drop", help = "Drops the given entity from the database. Usage: entity drop <schema>.<entity>") {
    /** Flag indicating whether CLI should ask for confirmation. */
    private val force: Boolean by option(
        "-f",
        "--force",
        help = "Forces the drop and does not ask for confirmation."
    ).flag(default = false)

    /** Flag that can be used to directly provide confirmation. */
    private val confirm: Boolean by option(
        "-c",
        "--confirm",
        help = "Directly provides the confirmation option."
    ).convert {
        it.toLowerCase() == "y"
    }.prompt(
        "Do you really want to drop the entity ${this.entityName} [y/N]?",
        default = "n",
        showDefault = false
    )

    override fun exec() {
        if (this.confirm) {
            try {
                val timedTable = measureTimedValue {
                    TabulationUtilities.tabulate(
                        this.ddlStub.dropEntity(
                            CottontailGrpc.DropEntityMessage.newBuilder()
                                .setEntity(this.entityName.proto()).build()
                        )
                    )
                }
                println("Successfully dropped entity ${this.entityName} (took ${timedTable.duration}).")
                print(timedTable.value)
            } catch (e: StatusException) {
                println("Failed to drop entity ${this.entityName} due to error: ${e.message}.")
            }
        } else {
            println("Drop entity aborted.")
        }
    }
}