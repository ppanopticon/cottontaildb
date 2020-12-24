package org.vitrivr.cottontail.cli.query

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.jakewharton.picnic.Table
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DQLGrpc
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.TimedValue
import kotlin.time.measureTimedValue

/**
 * Base class for commands that issue a query.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.2
 */
@ExperimentalTime
abstract class AbstractQueryCommand(private val stub: DQLGrpc.DQLBlockingStub, name: String, help: String) : AbstractCottontailCommand(name, help) {
    /** Flag indicating, whether query should be executed or explained. */
    private val explain: Boolean by option("-e", "--explain", help = "If set, query will only be executed and not set.").convert { it.toBoolean() }.default(false)

    /**
     * Takes a [CottontailGrpc.QueryMessage], executes and collects all results into a [List]
     * of [CottontailGrpc.Tuple] and measures the time to execute this action.
     *
     * @param query The query to collect.
     * @return [TimedValue] of the query response.
     */
    protected fun executeAndTabulate(query: CottontailGrpc.QueryMessage): TimedValue<Table> = measureTimedValue {
        if (this.explain) {
            TabulationUtilities.tabulate(this.stub.explain(query))
        } else {
            TabulationUtilities.tabulate(this.stub.query(query))
        }
    }
}