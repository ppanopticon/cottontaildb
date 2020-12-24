package org.vitrivr.cottontail

import kotlinx.serialization.json.Json
import org.vitrivr.cottontail.cli.Cli
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.server.grpc.CottontailGrpcServer
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.time.ExperimentalTime

/**
 * Entry point for Cottontail DB demon and CLI.
 *
 * @param args Program arguments.
 */
@ExperimentalTime
fun main(args: Array<String>) {

    /** Check if only CLI should be started. */
    if (args.isNotEmpty() && args[0] in arrayOf("prompt", "cli")) {
        if (args.size < 3) {
            println("Starting CLI on default setup")
            Cli().loop()
        }
        Cli(args[1], args[2].toInt()).loop()
        return
    }

    /* Handle case when arguments are empty. */
    val configPath = if (args.isEmpty()) {
        System.err.println("No config path specified; using default config at ./config.json.")
        Paths.get("./config.json") /* Start with default value. */
    } else {
        Paths.get(args.first()) /* Start with default value. */
    }

    /* Load config file and start Cottontail DB. */
    Files.newBufferedReader(configPath).use { reader ->
        val config = Json.decodeFromString(Config.serializer(), reader.readText())
        standalone(config)
    }
}

/**
 * Traditional Cottontail DB server startup method (standalone).
 *
 * @param config The [Config] object to start Cottontail DB with.
 */
@ExperimentalTime
fun standalone(config: Config) {
    /* Instantiate Catalogue, execution engine and gRPC server. */
    val catalogue = Catalogue(config)
    val engine = TransactionManager(config.execution)
    val server = CottontailGrpcServer(config.server, catalogue, engine)

    /* Start gRPC Server. */
    server.start()

    /* Start CLI (if configured). */
    if (config.cli) {
        Cli("localhost", config.server.port).loop()
        server.stop()
    }

    /* Wait for gRPC server to be stopped. */
    while (server.isRunning) {
        Thread.sleep(1000)
    }
}

/**
 * Cottontail DB server startup method for embedded mode.
 *
 * @param config The [Config] object to start Cottontail DB with.
 * @return [CottontailGrpcServer] instance.
 */
@ExperimentalTime
fun embedded(config: Config): CottontailGrpcServer {
    /* Instantiate Catalogue, execution engine and gRPC server. */
    val catalogue = Catalogue(config)
    val engine = TransactionManager(config.execution)
    val server = CottontailGrpcServer(config.server, catalogue, engine)

    /* Start gRPC Server and return object. */
    server.start()
    return server
}