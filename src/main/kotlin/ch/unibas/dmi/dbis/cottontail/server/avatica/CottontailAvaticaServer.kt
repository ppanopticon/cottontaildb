package ch.unibas.dmi.dbis.cottontail.server.avatica

import ch.unibas.dmi.dbis.cottontail.calcite.adapter.CottontailCatalogueFactory
import ch.unibas.dmi.dbis.cottontail.config.ServerConfig
import ch.unibas.dmi.dbis.cottontail.server.Server
import ch.unibas.dmi.dbis.cottontail.server.grpc.CottontailGrpcServer

import org.apache.calcite.avatica.jdbc.JdbcMeta
import org.apache.calcite.avatica.remote.Driver
import org.apache.calcite.avatica.remote.LocalService
import org.apache.calcite.avatica.server.HttpServer
import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.config.CalciteConnectionProperty
import org.apache.calcite.model.JsonSchema
import org.apache.log4j.LogManager
import org.apache.log4j.Logger

import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

import kotlin.system.exitProcess

/**
 * Implementation of Cottontail DB's Apache Avatica Server.
 *
 * @version 1.0
 * @author Ralph Gasser
 */
class CottontailAvaticaServer(val config: ServerConfig) : Server {

    /** Companion object with Logger reference. */
    companion object {
        /** Instance used for logging. */
        val LOGGER: Logger = LogManager.getLogger(CottontailAvaticaServer::class.qualifiedName)

        /** Hard coded properties used to connect to Cottontail DB through local Apache Calcite instance. */
        val INFO: Properties = Properties()
        init {
            INFO[CalciteConnectionProperty.SCHEMA.camelName()] = "warren"
            INFO[CalciteConnectionProperty.SCHEMA_TYPE.camelName()] = JsonSchema.Type.CUSTOM.toString()
            INFO[CalciteConnectionProperty.SCHEMA_FACTORY.camelName()] = CottontailCatalogueFactory::class.java.name
            INFO[CalciteConnectionProperty.QUOTING.camelName()] = "DOUBLE_QUOTE"
            INFO[CalciteConnectionProperty.UNQUOTED_CASING.camelName()] = Casing.TO_LOWER.toString()
            INFO[CalciteConnectionProperty.QUOTED_CASING.camelName()] = Casing.TO_LOWER.toString()
            INFO[CalciteConnectionProperty.CASE_SENSITIVE.camelName()] = "false"
        }
    }

    /** The JDBC URL for connecting to Cottontail DB. */
    private val service = LocalService(JdbcMeta("jdbc:calcite:", INFO))

    /** Avatica Server Instance. */
    private val server = HttpServer.Builder<Any>()
            .withHandler(this.service, Driver.Serialization.PROTOBUF)
            .withPort(this.config.port)
            .build()

    /** Flag indicating, whether this instance of CottontailAvaticaServer is running. */
    private val running = AtomicBoolean(false)

    /**
     * Returns true if this [CottontailAvaticaServer] is currently running, and false otherwise.
     */
    override val isRunning: Boolean
        get() = this.running.get()

    /**
     * Starts this instance of [CottontailAvaticaServer].
     */
    override fun start() {
        try {
            if (!this.running.get()) {
                this.server.start()
                this.running.set(true)
                LOGGER.info("Cottontail DB Avatica server is up and running at port ${this.config.port}! Hop along...")
            }
        } catch (e: Exception) {
            LOGGER.info("Cottontail DB Avatica server could not be started due to an exception: ${e.message}.")
            exitProcess(2)
        }
    }

    /**
     * Stops this instance of [CottontailAvaticaServer].
     */
    override fun stop() {
        this.server.stop()
        this.running.set(false)
        CottontailGrpcServer.LOGGER.info("Cottontail DB was shut down. Have a binky day!")
        exitProcess(0)
    }
}