package org.vitrivr.cottontail.cli

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.NoOpCliktCommand
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.output.CliktHelpFormatter
import com.github.ajalt.clikt.output.HelpFormatter
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.int
import com.github.ajalt.clikt.parameters.types.long

import com.jakewharton.picnic.table
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.StatusRuntimeException

import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.grpc.CottonDMLGrpc
import org.vitrivr.cottontail.grpc.CottonDQLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc

import kotlin.system.exitProcess
import kotlin.time.ExperimentalTime

/**
 * Wrapper class and single access point to the actual commands.  Also, provides the gRPC bindings
 *
 * <strong>How to add more commands:</strong>
 * <ul>
 *     <li>Write dedicated inner class (extend [AbstractEntityCommand] if it's an entity specific command, otherwise [AbstractCottontailCommand])</li>
 *     <li>Add this command to subcommands call in line 32ff (init of this class)</li>
 * </ul>
 *
 * @author Loris Sauter
 * @version 1.0.1
 */
@ExperimentalTime
class CottontailCommand(private val host: String, private val port: Int) : NoOpCliktCommand(name = "cottontail", help = "The base command") {

    init {
        context { helpFormatter = CliHelpFormatter() }
        subcommands(
                ListAllEntitiesCommand(),
                PreviewEntityCommand(),
                TabulatedPreviewEntityCommand(),
                ShowEntityCommand(),
                DropEntityCommand(),
                OptimizeEntityCommand(),
                CountEntityCommand(),
                QueryByColumnValueEqualsEntityCommand(),
                ReloadCommand(),
                StopCommand())

        initDbCon(host, port)
    }

    lateinit var channel: ManagedChannel
    lateinit var dqlService: CottonDQLGrpc.CottonDQLBlockingStub
    lateinit var ddlService: CottonDDLGrpc.CottonDDLBlockingStub
    lateinit var dmlService: CottonDMLGrpc.CottonDMLBlockingStub


    /**
     * Initialises the db connection
     */
    private fun initDbCon(host: String, port: Int) {
        if (::channel.isInitialized) {
            // Its a reload. Close all
            channel.shutdownNow()
        }
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()

        dqlService = CottonDQLGrpc.newBlockingStub(channel)
        ddlService = CottonDDLGrpc.newBlockingStub(channel)
        dmlService = CottonDMLGrpc.newBlockingStub(channel)
    }

    fun initCompletion() {
        val schemata = mutableListOf<CottontailGrpc.Schema>()
        val entities = mutableListOf<CottontailGrpc.Entity>()
        this@CottontailCommand.ddlService.listSchemas(CottontailGrpc.Empty.getDefaultInstance()).forEach { _schema ->
            schemata.add(_schema)
            this@CottontailCommand.ddlService.listEntities(_schema).forEach { _entity ->
                if (_entity.schema.name != _schema.name) {
                    // something is wrong. We ignore it currently
                }
                entities.add(_entity)
            }
        }
        Cli.updateArgumentCompletion(schemata = schemata.map { it.name }, entities = entities.map { it.name })
    }


    /**
     * Base class for none entity specific commands (for potential future generalisation)
     */
    abstract inner class AbstractCottontailCommand(name: String, help: String) : CliktCommand(name = name, help = help) {
        /**
         * The actual command execution. Override this for your command
         */
        abstract fun exec()

        /**
         * Runs the command by calling [AbstractCottontailCommand.exec] safely. Exceptions are caught and printed.
         */
        override fun run() = try {
            exec()
        } catch (e: StatusRuntimeException) {
            println("Command execution failed: ${e.message}")
        }
    }

    /**
     * Base class for entity specific commands, providing / querying from user schema and entity as arguments
     */
    abstract inner class AbstractEntityCommand(name: String, help: String) : AbstractCottontailCommand(name = name, help = help) {
        protected val schema: String by argument(name = "schema", help = "The schema name this command targets at")
        protected val entity: String by argument(name = "entity", help = "The entity name this command targets at")
    }

    /**
     * Command to list available entities and schemata
     */
    inner class ListAllEntitiesCommand : AbstractCottontailCommand(name = "list", help = "Lists all entities in all schemata") {
        override fun exec() {
            this@CottontailCommand.ddlService.listSchemas(CottontailGrpc.Empty.getDefaultInstance()).forEach { _schema ->
                println("Entities for Schema ${_schema.name}:")
                this@CottontailCommand.ddlService.listEntities(_schema).forEach { _entity ->
                    if (_entity.schema.name != _schema.name) {
                        println("Data integrity threat! entity $_entity ist returned when listing entities for schema $_schema")
                    }
                    println("  ${_entity.name}")
                }
            }
        }
    }

    /**
     * Command to preview a given entity
     */
    inner class PreviewEntityCommand : AbstractEntityCommand(name = "preview", help = "Gives a preview of the entity specified") {
        // FIXME the option clashes with the current completion handling
        private val limit: Long by option("-l", "--limit", help = "Limits the amount of printed results").long().default(10).validate { require(it > 1) }
        override fun exec() {
            val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                    CottontailGrpc.Query.newBuilder()
                            .setFrom(CottontailGrpc.From.newBuilder().setEntity(CottontailGrpc.Entity.newBuilder().setName(entity).setSchema(CottontailGrpc.Schema.newBuilder().setName(schema))))
                            .setProjection(MatchAll())
                            .setLimit(limit)
            ).build()
            val query = this@CottontailCommand.dqlService.query(qm)
            println("Previewing $limit elements of $entity")
            query.forEach { page ->
                println(page.resultsList.dropLast(Math.max(0, page.resultsCount - limit).toInt()))
            }
        }
    }

    inner class TabulatedPreviewEntityCommand : AbstractEntityCommand(name = "table", help = "Tabulated preview of an entity") {
        private val limit: Long by option("-l", "--limit", help = "Limits the amount of printed results").long().default(10).validate { require(it > 1) }

        override fun exec() {
            println("Presenting first $limit elements of entity $schema.$entity")
            val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                    CottontailGrpc.Query.newBuilder()
                            .setFrom(CottontailGrpc.From.newBuilder().setEntity(CottontailGrpc.Entity.newBuilder().setName(entity).setSchema(CottontailGrpc.Schema.newBuilder().setName(schema))))
                            .setProjection(MatchAll())
                            .setLimit(this.limit)
            ).build()
            val query = this@CottontailCommand.dqlService.query(qm)

            val tuples = mutableListOf<CottontailGrpc.Tuple>()
            query.forEach {
                it.resultsList.forEach {
                    tuples.add(it)
                }
            }
            if (tuples.size > 0) {
                tabulate(tuples)
            } else {
                println("No results!")
            }
        }

        private fun tabulate(tuples: List<CottontailGrpc.Tuple>) {
            val tbl = table {
                cellStyle {
                    border = true
                    paddingLeft = 1
                    paddingRight = 1
                }
                header {
                    row {
                        tuples.first().dataMap.keys.forEach {
                            this.cell(it)
                        }
                    }
                }
                body {
                    tuples.forEach {
                        row {
                            it.dataMap.values.map {
                                when (it.dataCase) {
                                    CottontailGrpc.Data.DataCase.BOOLEANDATA -> it.booleanData.toString()
                                    CottontailGrpc.Data.DataCase.INTDATA -> it.intData.toString()
                                    CottontailGrpc.Data.DataCase.LONGDATA -> it.longData.toString()
                                    CottontailGrpc.Data.DataCase.FLOATDATA -> it.floatData.toString()
                                    CottontailGrpc.Data.DataCase.DOUBLEDATA -> it.doubleData.toString()
                                    CottontailGrpc.Data.DataCase.STRINGDATA -> it.stringData
                                    CottontailGrpc.Data.DataCase.COMPLEX32DATA -> it.complex32Data.toString()
                                    CottontailGrpc.Data.DataCase.COMPLEX64DATA -> it.complex64Data.toString()
                                    CottontailGrpc.Data.DataCase.VECTORDATA -> it.vectorData.toString() // fixme not sure whether this is reasonable
                                    CottontailGrpc.Data.DataCase.NULLDATA -> "~~NULL~~"
                                    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> "~~N/A~~"
                                    else -> ""
                                }
                            }.forEach { cell(it) }
                        }
                    }
                }
            }
            println(tbl)
        }
    }

    /**
     * COmmand for entity details
     */
    inner class ShowEntityCommand : AbstractEntityCommand(name = "show", help = "Gives an overview of the entity and its columns") {
        override fun exec() {
            val details = this@CottontailCommand.ddlService.entityDetails(CottontailGrpc.Entity.newBuilder().setName(entity).setSchema(CottontailGrpc.Schema.newBuilder().setName(schema)).build())
            println("Entity ${details.entity.schema.name}.${details.entity.name} with ${details.columnsCount} columns: ")
            print("  ${details.columnsList.map { "${it.name} (${it.type})" }.joinToString(", ")}")
        }
    }

    /**
     * Command to optimize an entity
     */
    inner class OptimizeEntityCommand : AbstractEntityCommand(name = "optimize", help = "Optimizes the specified entity, e.g. rebuilds the indices") {
        override fun exec() {
            println("Optimizing entity $schema.$entity")
            this@CottontailCommand.ddlService.optimize(CottontailGrpc.Entity.newBuilder().setName(entity).setSchema(CottontailGrpc.Schema.newBuilder().setName(schema)).build())
            println("Optimization complete!")
        }
    }

    inner class CountEntityCommand : AbstractEntityCommand(name = "count", help = "Counts the given entity's rows") {
        override fun exec() {
            println("Counting elements of entity $schema.$entity")
            val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                    CottontailGrpc.Query.newBuilder()
                            .setFrom(CottontailGrpc.From.newBuilder().setEntity(CottontailGrpc.Entity.newBuilder().setName(entity).setSchema(CottontailGrpc.Schema.newBuilder().setName(schema))))
                            .setProjection(CottontailGrpc.Projection.newBuilder().setOp(CottontailGrpc.Projection.Operation.COUNT).build())

            ).build()
            val query = this@CottontailCommand.dqlService.query(qm)
            query.forEach { page ->
                if (page.resultsList.isNotEmpty()) {
                    println(page.resultsList.first())
                }
            }
        }
    }

    inner class QueryByColumnValueEqualsEntityCommand : AbstractEntityCommand(name = "find", help = "Find within an entity by column-value specification") {
        val col: String by option("-c", "--column", help = "Column name").required()
        val value: String by option("-v", "--value", help = "The value").required()
        override fun exec() {
            val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                    CottontailGrpc.Query.newBuilder()
                            .setFrom(CottontailGrpc.From.newBuilder().setEntity(CottontailGrpc.Entity.newBuilder().setName(entity).setSchema(CottontailGrpc.Schema.newBuilder().setName(schema))))
                            .setProjection(MatchAll())
                            .setWhere(Where(
                                    CottontailGrpc.AtomicLiteralBooleanPredicate.newBuilder()
                                            .setAttribute(col)
                                            .setOp(CottontailGrpc.AtomicLiteralBooleanPredicate.Operator.EQUAL)
                                            .addData(CottontailGrpc.Data.newBuilder().setStringData(value).build())
                                            .build()
                            ))
            ).build()
            val query = this@CottontailCommand.dqlService.query(qm)
            var size = 0
            query.forEach { page ->
                println(page.resultsList)
                size += page.resultsCount
            }
            println("Printed $size individual results")
        }
    }

    /**
     * Command to drop, i.e. remove the given entity
     */
    inner class DropEntityCommand : AbstractEntityCommand(name = "drop", help = "Drops the given entity from the database and deletes it therefore") {
        override fun exec() {
            this@CottontailCommand.ddlService.dropEntity(CottontailGrpc.Entity.newBuilder().setName(entity).setSchema(CottontailGrpc.Schema.newBuilder().setName(schema)).build())
            println("Dropped entity $schema.$entity")
        }
    }

    /**
     * Dedicated help formatter
     */
    inner class CliHelpFormatter : CliktHelpFormatter() {
        override fun formatHelp(
                prolog: String,
                epilog: String,
                parameters: List<HelpFormatter.ParameterHelp>,
                programName: String): String = buildString {
            if (programName.contains(" ")) {
                addUsage(parameters, programName.split(" ")[1]) // hack to not include the base command
            } else {
                addUsage(parameters, programName)
            }
            addOptions(parameters)
            addArguments(parameters)
            addCommands(parameters)
        }
    }

    /**
     * Stops the entire application
     */
    inner class StopCommand : AbstractCottontailCommand(name = "stop", help = "Stops the database server and this CLI") {
        override fun exec() {
            println("Stopping now...")
            this@CottontailCommand.channel.shutdown()
            Cli.stopServer()
            exitProcess(0)
        }
    }

    /**
     * Reconnects to Cottontail DB server.
     */
    inner class ReloadCommand : AbstractCottontailCommand(name = "reload", help = "Reload the connection to the DB server") {
        val host: String by option("-s", "--server", help = "The server address.").defaultLazy { this@CottontailCommand.host }
        val port: Int by option("-p", "--port", help = "The db port").int().defaultLazy { this@CottontailCommand.port }
        override fun exec() {
            println("Reconnecting...")
            this@CottontailCommand.initDbCon(host, port)
            println("Reconnected.")
        }
    }
}