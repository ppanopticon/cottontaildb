package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.enum
import io.grpc.StatusException
import org.vitrivr.cottontail.database.queries.binding.extensions.fqn
import org.vitrivr.cottontail.database.queries.binding.extensions.proto
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to create a index on a specified entities column from Cottontail DB.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.3
 */
@ExperimentalTime
class CreateIndexCommand(private val ddlStub: DDLGrpc.DDLBlockingStub) : AbstractEntityCommand(name = "create-index", help = "Creates an index on the given entity and rebuilds the newly created index. Usage: entity createIndex <schema>.<entity> <column> <index>") {

    private val attribute by argument(
        name = "column",
        help = "The column name to create the index for"
    )
    private val index by argument(
        name = "index",
        help = "The index to create"
    ).enum<CottontailGrpc.IndexType>()
    private val rebuild: Boolean by option(
        "-r",
        "--rebuild",
        help = "Limits the amount of printed results"
    )
        .flag(default = false)

    override fun exec() {
        val entity = entityName.proto()
        val index = CottontailGrpc.IndexDefinition.newBuilder()
            .setType(this.index)
            .setName(
                CottontailGrpc.IndexName.newBuilder().setEntity(entity)
                    .setName("index-${index.name.toLowerCase()}-${entityName.schema()}_${entity.name}_${attribute}")
            )
            .addColumns(CottontailGrpc.ColumnName.newBuilder().setName(this.attribute))
            .build()

        try {
            val timedTable = measureTimedValue {
                TabulationUtilities.tabulate(this.ddlStub.createIndex(CottontailGrpc.CreateIndexMessage.newBuilder().setRebuild(this.rebuild).setDefinition(index).build()))
            }
            println("Successfully created index ${index.name.fqn()} (took ${timedTable.duration}).")
            print(timedTable.value)
        } catch (e: StatusException) {
            println("Error while creating index ${index.name.fqn()}: ${e.message}.")
        }
    }
}