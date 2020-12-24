package org.vitrivr.cottontail.cli.query

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import io.grpc.StatusException
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DQLGrpc
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.server.grpc.helper.protoFrom
import kotlin.time.ExperimentalTime

/**
 * Counts the number of rows in an [org.vitrivr.cottontail.database.entity.Entity].
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.2
 */
@ExperimentalTime
class CountEntityCommand(dqlStub: DQLGrpc.DQLBlockingStub) : AbstractQueryCommand(name = "count", help = "Counts the number of entries in the given entity. Usage: entity count <schema>.<entity>", stub = dqlStub) {

    private val entityName: Name.EntityName by argument(name = "entity", help = "The fully qualified entity name targeted by the command. Has the form of [\"warren\"].<schema>.<entity>").convert {
        Name.EntityName(*it.split(Name.NAME_COMPONENT_DELIMITER).toTypedArray())
    }

    override fun exec() {
        val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                CottontailGrpc.Query.newBuilder()
                        .setFrom(this.entityName.protoFrom())
                        .setProjection(CottontailGrpc.Projection.newBuilder().setOp(CottontailGrpc.Projection.ProjectionOperation.COUNT).build())
        ).build()

        /* Execute and prepare table. */
        try {
            val results = this.executeAndTabulate(qm)

            /* Print. */
            println("Counting elements of  ${this.entityName} (took: ${results.duration}):")
            print(results.value)
        } catch (e: StatusException) {
            println("Failed to count elements of ${this.entityName} due to error: ${e.message}")
        }
    }
}