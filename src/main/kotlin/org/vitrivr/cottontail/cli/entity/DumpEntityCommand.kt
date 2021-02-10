package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import org.vitrivr.cottontail.database.queries.binding.extensions.protoFrom
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DQLGrpc
import org.vitrivr.cottontail.utilities.data.Format
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Command to dump the content of an entity into a file.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@ExperimentalTime
class DumpEntityCommand(private val dql: DQLGrpc.DQLBlockingStub) :
    AbstractEntityCommand(name = "dump", help = "Dumps the content of an entire entity to disk.") {

    /** Flag indicating, whether query should be executed or explained. */
    private val out: Path by option(
        "-o",
        "--out",
        help = "Path to the output folder (file name will be determined by entity and format)."
    ).convert { Paths.get(it) }.required()

    /** Flag indicating, whether query should be executed or explained. */
    private val format: Format by option(
        "-f",
        "--format",
        help = "Export format. Defaults to PROTO."
    ).convert { Format.valueOf(it) }.default(Format.PROTO)

    override fun exec() {
        val qm = CottontailGrpc.QueryMessage.newBuilder()
            .setQuery(CottontailGrpc.Query.newBuilder().setFrom(this.entityName.protoFrom()))
            .build()
        val path = this.out.resolve("${this.entityName}.${this.format.suffix}")
        val dataExporter = this.format.newExporter(path)

        try {
            val duration = measureTime {
                /* Execute query and export data. */
                val results = this.dql.query(qm)
                for (r in results) {
                    dataExporter.offer(r)
                }
                dataExporter.close()
            }
            println("Dumping ${this.entityName} took $duration.")
        } catch (e: Throwable) {
            print("A ${e::class.java.simpleName} occurred while executing and exporting query: ${e.message}.")
        }
    }
}