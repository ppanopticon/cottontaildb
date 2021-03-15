package org.vitrivr.cottontail.cli.system

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.int
import kotlinx.serialization.json.Json
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.legacy.v1.MigrationManagerV1
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.time.ExperimentalTime

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@ExperimentalTime
class MigrationCommand : AbstractCottontailCommand(
    name = "migration",
    help = "Command to migrate between Cottontail DB instances."
) {

    /** Path to .proto file that contains query. */
    private val input: Path by option(
        "-i",
        "--input",
        help = "Path to the config file used for input."
    ).convert { Paths.get(it) }.required()

    /** Path to .proto file that contains query. */
    private val log: Path by option(
        "-l",
        "--log",
        help = "Path to the destination for the log file. Defaults the current folder."
    ).convert { Paths.get(it) }.default(Paths.get("."))

    /** Path to .proto file that contains query. */
    private val batchSize: Int by option(
        "-b",
        "--batch",
        help = "How many entries should be batched before issuing a commit when migrating entities. Defaults to 1'000'000 entries."
    ).int().default(1_000_000)


    /**
     * Executes the data migration.
     */
    override fun exec() {
        Files.newBufferedReader(input).use { reader ->
            val config = Json.decodeFromString(Config.serializer(), reader.readText())
            MigrationManagerV1(this.batchSize, this.log).use { it.migrate(config) }
        }
    }
}