package org.vitrivr.cottontail.cli.query

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import org.vitrivr.cottontail.cli.MatchAll
import org.vitrivr.cottontail.cli.Where
import org.vitrivr.cottontail.database.queries.binding.extensions.protoFrom
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DQLGrpc
import org.vitrivr.cottontail.model.basics.Name
import kotlin.time.ExperimentalTime

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@ExperimentalTime
class FindInEntityCommand(dqlStub: DQLGrpc.DQLBlockingStub) : AbstractQueryCommand(name = "find", help = "Find within an entity by column-value specification", stub = dqlStub) {

    private val entityName: Name.EntityName by argument(name = "entity", help = "The fully qualified entity name targeted by the command. Has the form of [\"warren\"].<schema>.<entity>").convert {
        Name.EntityName(*it.split(Name.NAME_COMPONENT_DELIMITER).toTypedArray())
    }
    val col: String by option("-c", "--column", help = "Column name").required()
    val value: String by option("-v", "--value", help = "The value").required()
    override fun exec() {
        val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
            CottontailGrpc.Query.newBuilder()
                .setFrom(this.entityName.protoFrom())
                .setProjection(MatchAll())
                .setWhere(
                    Where(
                        CottontailGrpc.AtomicLiteralBooleanPredicate.newBuilder()
                            .setLeft(CottontailGrpc.ColumnName.newBuilder().setName(this.col))
                            .setOp(CottontailGrpc.ComparisonOperator.EQUAL)
                            .addRight(
                                CottontailGrpc.Literal.newBuilder().setStringData(this.value)
                            )
                            .build()
                    )
                )
        ).build()

        /* Execute query based on options. */
        if (this.toFile) {
            this.executeAndExport(qm)
        } else {
            this.executeAndTabulate(qm)
        }
    }
}