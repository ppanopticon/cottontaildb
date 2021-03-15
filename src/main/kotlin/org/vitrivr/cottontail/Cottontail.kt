package org.vitrivr.cottontail

import kotlinx.serialization.json.Json
import org.vitrivr.cottontail.cli.Cli
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.legacy.VersionProber
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
            return
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

    /* Check if config file exists. */
    if (!Files.isRegularFile(configPath)) {
        System.err.println("Specified Cottontail DB configuration file $configPath does not exist. Cottontail DB will shut down.")
        exitProcess(1)
    }

    /* Load config file and start Cottontail DB. */
    Files.newBufferedReader(configPath).use { reader ->
        val config = Json.decodeFromString(Config.serializer(), reader.readText())
        if (!Files.isDirectory(config.root)) {
            System.err.println("Specified Cottontail DB data folder file $${config.root} does not exist. Cottontail DB will shut down.")
            exitProcess(1)
        }
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

    /* Check catalogue version. */
    val detected = VersionProber(config).probe(config.root)
    if (detected == DBOVersion.UNDEFINED) {
        System.err.println("Folder does not seem to be a valid Cottontail DB data folder (detected = $detected, path = ${config.root}.")
        exitProcess(1)
    } else if (detected != VersionProber.EXPECTED) {
        System.err.println("Version mismatch: Trying to open an incompatible Cottontail DB catalogue version. Run system migration to upgrade catalogue (detected = $detected, expected = ${VersionProber.EXPECTED}, path = ${config.root}).")
        exitProcess(1)
    }

    /* Instantiate Catalogue, execution engine and gRPC server. */
    val catalogue = DefaultCatalogue(config)

    /* Start gRPC Server and print message. */
    val server = CottontailGrpcServer(config, catalogue)
    server.start()
    println("Cottontail DB server is up and running at port ${config.server.port}! Hop along... (catalogue: $detected, pid: ${ProcessHandle.current().pid()})")

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
    val catalogue = DefaultCatalogue(config)
    val server = CottontailGrpcServer(config, catalogue)

    /* Start gRPC Server and return object. */
    server.start()
    return server
}