package org.vitrivr.cottontail

import kotlinx.serialization.json.Json
import org.vitrivr.cottontail.cli.Cli
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.server.grpc.CottontailGrpcServer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.system.exitProcess
import kotlin.time.ExperimentalTime

/**
 * Entry point for Cottontail DB demon and CLI.
 *
 * @param args Program arguments.
 */
@ExperimentalTime
fun main(args: Array<String>) {

    /* Handle case when arguments are empty. */
    if (args.isEmpty()) {
        System.err.println("No config path specified; using default config at ./config.json.")
        standalone(Paths.get("./config.json")) /* Start with default value. */
    }

    /* Check the arguments and start Cottontail DB accordingly */
    when (args[0].toLowerCase()) {
        "cli", "prompt" -> {
            if (args.size < 3) {
                println("To start the CLI start Cottontail DB use\n" +
                        "$> cli [<host>] [<port>]")
                exitProcess(1)
            }
            Cli.loop(args[1], args[2].toInt())
        }
        else -> standalone(Paths.get(args[0]))
    }
}

/**
 * Traditional Cottontail DB server startup method (standalone).
 *
 * @param configPath The [Path] to the configuration file.
 */
@ExperimentalTime
fun standalone(configPath: Path) {
    /* Load config file and start Cottontail DB. */
    Files.newBufferedReader(configPath).use { reader ->
        val config = Json.decodeFromString(Config.serializer(), reader.readText())
        val catalogue = Catalogue(config)
        val engine = ExecutionEngine(config.execution)
        val server = CottontailGrpcServer(config.server, catalogue, engine)
        server.start()
        if (config.cli) {
            /* Start local cli */
            Cli.cottontailServer = server
            Cli.loop("localhost", config.server.port)
        } else {
            while (server.isRunning) {
                Thread.sleep(1000)
            }
        }
    }
}

/**
 * Cottontail DB server startup method for embedded mode.
 *
 * @param configPath The [Path] to the configuration file.
 * @return [CottontailGrpcServer] instance.
 */
@ExperimentalTime
fun embedded(configPath: String = "config.json"): CottontailGrpcServer {
    /* Load config file and start Cottontail DB. */
    Files.newBufferedReader(Paths.get(configPath)).use { reader ->
        val config = Json.decodeFromString(Config.serializer(), reader.readText())
        val catalogue = Catalogue(config)
        val engine = ExecutionEngine(config.execution)
        val server = CottontailGrpcServer(config.server, catalogue, engine)
        server.start()
        return server
    }
}