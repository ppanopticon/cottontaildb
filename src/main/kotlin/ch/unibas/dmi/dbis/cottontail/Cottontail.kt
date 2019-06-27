package ch.unibas.dmi.dbis.cottontail

import ch.unibas.dmi.dbis.cottontail.config.Config
import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.server.ServerEnum
import ch.unibas.dmi.dbis.cottontail.server.avatica.CottontailAvaticaServer

import kotlinx.serialization.json.Json

import java.nio.file.Files
import java.nio.file.Paths

/**
 * Cottontail DB's main class.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
internal object Cottontail {
    /** Global (singleton) instance of Cottontail DB's [Config] */
    var CONFIG: Config? = null
        private set

    /** Global (singleton) instance of Cottontail DB's [Catalogue] */
    var CATALOGUE: Catalogue? = null
        private set

    /**
     * Main method; application starting point.
     *
     * @param args Program arguments.
     */
    @JvmStatic
    fun main(args: Array<String>) {
        /* Check, if args were set properly. */
        val path = Paths.get(if (args.isEmpty()) {
            System.err.println("No config path specified, taking default config at config.json")
            "config.json"
        } else {
            args[0]
        })

        /* Load config file and start Cottontail DB. */
        val config = Files.newBufferedReader(path).use { reader ->
            val config = Json.parse(Config.serializer(), reader.readText())
            CONFIG = config
            CATALOGUE = Catalogue(config)
            config
        }

        /* Instantiate server according to configuration. */
        val server = when (config.serverConfig.server) {
            ServerEnum.AVATICA -> CottontailAvaticaServer(CONFIG!!.serverConfig)
            else -> TODO("Not implemented yet!")
        }
        server.start()

        /* Poll, while server is running. */
        while(server.isRunning) {
            Thread.sleep(500)
        }
    }
}