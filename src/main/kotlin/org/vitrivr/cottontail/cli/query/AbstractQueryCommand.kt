package org.vitrivr.cottontail.cli.query

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.jakewharton.picnic.Table
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DQLGrpc
import org.vitrivr.cottontail.utilities.data.Format
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime
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
    private val explain: Boolean by option(
        "-e",
        "--explain",
        help = "If set, query will only be executed and not set."
    ).convert { it.toBoolean() }.default(false)

    /** Flag indicating, whether query should be executed or explained. */
    private val out: Path? by option(
        "-o",
        "--out",
        help = "If set, query will be exported into files instead of CLI."
    ).convert { Paths.get(it) }

    /** Flag indicating, whether query should be executed or explained. */
    private val format: Format? by option(
        "-f",
        "--format",
        help = "Only for option --out; export format. Defaults to PROTO"
    ).convert { Format.valueOf(it) }

    /** Flag indicating, whether query should written to file or console. */
    protected val toFile: Boolean
        get() = (this.out != null)

    /**
     * Takes a [CottontailGrpc.QueryMessage], executes and collects all results into a [Table] and
     * prints it to stdout.
     *
     * @param query The query to execute and print..
     */
    protected fun executeAndTabulate(query: CottontailGrpc.QueryMessage) {
        try {
            val results = measureTimedValue {
                if (this.explain) {
                    TabulationUtilities.tabulate(this.stub.explain(query))
                } else {
                    TabulationUtilities.tabulate(this.stub.query(query))
                }
            }
            if (this.explain) {
                println("Explaining query (took: ${results.duration}):")
            } else {
                println("Executing query (took: ${results.duration}):")
            }
            print(results.value)
        } catch (e: Throwable) {
            print("An error occurred while executing query: ${e.message}.")
        }
    }

    /**
     * Takes a [CottontailGrpc.QueryMessage], executes and collects all results into a specified
     * output file using the specified format.
     *
     * @param query The query to execute and export.
     */
    protected fun executeAndExport(query: CottontailGrpc.QueryMessage) {
        try {
            val duration = measureTime {
                if (this.out != null) {
                    /* Determine which data exporter to use. */
                    val dataExporter = this.format?.newExporter(this.out!!)
                    if (dataExporter == null) {
                        println("Valid output format needs to be specified for export!")
                        return
                    }

                    /* Execute query. */
                    val results = if (this.explain) {
                        this.stub.explain(query)
                    } else {
                        this.stub.query(query)
                    }

                    /* Export data. */
                    for (r in results) {
                        dataExporter.offer(r)
                    }
                    dataExporter.close()
                }
            }
            println("Executing and exporting query took $duration.")
        } catch (e: Throwable) {
            print("A ${e::class.java.simpleName} occurred while executing and exporting query: ${e.message}.")
        }
    }
}