package org.vitrivr.cottontail

import kotlinx.serialization.json.Json
import org.vitrivr.cottontail.cli.Cli
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.server.grpc.CottontailGrpcServer
import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.file.Files
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
        try {
            standalone(config)
        } catch (e: Throwable) {
            System.err.println("Failed to start Cottontail DB due to error:")
            System.err.println(e.printStackTrace())
            exitProcess(1)
        }
    }
}

/**
 * Traditional Cottontail DB server startup method (standalone).
 *
 * @param config The [Config] object to start Cottontail DB with.
 */
@ExperimentalTime
fun standalone(config: Config) {
    /* Prepare Log4j logging facilities. */
    if (config.logConfig != null && Files.isRegularFile(config.logConfig)) {
        System.getProperties().setProperty("log4j.configurationFile", config.logConfig.toString())
    }

    /* Instantiate Catalogue, execution engine and gRPC server. */
    val catalogue = Catalogue(config)
    val server = CottontailGrpcServer(config, catalogue)

    /* Start gRPC Server and print message. */
    server.start()
    println(
        "Cottontail DB server is up and running at port ${config.server.port}! Hop along... (PID: ${
            ProcessHandle.current().pid()
        })"
    )

    /* Start CLI (if configured). */
    if (config.cli) {
        Cli("localhost", config.server.port).loop()
        server.stop()
    } else {
        /* Wait for gRPC server to be stopped. */
        val obj = BufferedReader(InputStreamReader(System.`in`))
        loop@ while (server.isRunning) {
            when (obj.readLine()) {
                "exit", "quit", "stop" -> break
            }
            Thread.sleep(100)
        }
    }

    /* Print shutdown message. */
    println("Cottontail DB was shut down. Have a binky day!")
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
    val server = CottontailGrpcServer(config, catalogue)

    /* Start gRPC Server and return object. */
    server.start()
    return server
}