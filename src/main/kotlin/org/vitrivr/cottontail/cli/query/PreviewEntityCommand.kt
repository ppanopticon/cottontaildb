package org.vitrivr.cottontail.cli.query

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.validate
import com.github.ajalt.clikt.parameters.types.long
import org.vitrivr.cottontail.cli.MatchAll
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DQLGrpc
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.server.grpc.helper.protoFrom
import kotlin.time.ExperimentalTime

/**
 * Command to preview a given entity
 *
 * @author Loris Sauter
 * @version 1.0.1
 */
@ExperimentalTime
class PreviewEntityCommand constructor(dqlStub: DQLGrpc.DQLBlockingStub) : AbstractQueryCommand(name = "preview", help = "Gives a preview of the entity specified", stub = dqlStub) {

    private val entityName: Name.EntityName by argument(name = "entity", help = "The fully qualified entity name targeted by the command. Has the form of [\"warren\"].<schema>.<entity>").convert {
        Name.EntityName(*it.split(Name.NAME_COMPONENT_DELIMITER).toTypedArray())
    }
    private val limit: Long by option("-l", "--limit", help = "Limits the amount of printed results").long().default(50).validate { require(it > 1) }
    private val skip: Long by option("-s", "--skip", help = "Limits the amount of printed results").long().default(0).validate { require(it >= 0) }

    override fun exec() {
        /* Prepare query. */
        val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                CottontailGrpc.Query.newBuilder()
                        .setFrom(this.entityName.protoFrom())
                        .setProjection(MatchAll())
                        .setLimit(this.limit)
                        .setSkip(this.skip)
        ).build()

        /* Execute and prepare table. */
        val results = this.executeAndTabulate(qm)

        /* Print. */
        println("Previewing ${this.limit} elements of  ${this.entityName} (took: ${results.duration}):")
        println(results.value)
    }
}