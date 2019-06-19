package ch.unibas.dmi.dbis.cottontail

import ch.unibas.dmi.dbis.cottontail.calcite.adapter.CalciteCottontailDriver
import ch.unibas.dmi.dbis.cottontail.config.Config
import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.server.ServerEnum
import ch.unibas.dmi.dbis.cottontail.server.avatica.CottontailAvaticaServer

import kotlinx.serialization.json.Json
import org.apache.calcite.schema.impl.TableFunctionImpl
import org.apache.calcite.tools.Frameworks

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager

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
        /* Trick to load the JDBC driver. */
        Class.forName(CalciteCottontailDriver::class.java.canonicalName)

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

        val connection = DriverManager.getConnection("jdbc:cottontail:")
        val stmt = connection.prepareStatement("select knn(select * from features_AverageColor, 250, [1.0, 1.0, 1.0, 1.0]) from cineast.cineast_multimediaobject LIMIT 10")
        val results = stmt.executeQuery()

        while (results.next()) {
            println(results.getString(1))
        }


        /* Poll, while server is running. */
        while(server.isRunning) {
            Thread.sleep(500)
        }
    }
}