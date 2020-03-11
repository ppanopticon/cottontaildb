package ch.unibas.dmi.dbis.cottontail.cli

import ch.unibas.dmi.dbis.cottontail.grpc.CottonDDLGrpc
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDMLGrpc
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDQLGrpc
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc

import io.grpc.ManagedChannelBuilder


object DatabasePreview {


    val channel = ManagedChannelBuilder.forAddress("127.0.0.1", 1865).usePlaintext().build()

    val dqlService = CottonDQLGrpc.newBlockingStub(channel)
    val ddlService = CottonDDLGrpc.newBlockingStub(channel)
    val dmlService = CottonDMLGrpc.newBlockingStub(channel)

    val schema = CottontailGrpc.Schema.newBuilder().setName("test").build()
    val entity = CottontailGrpc.Entity.newBuilder()
            .setSchema(schema)
            .setName("surf")
            .build()


    @JvmStatic
    fun main(args: Array<String>) {

        loop@ while (true) {
            try {
                println("Please enter a command. help gives an overview, quit also exists")
                val command = readLine()!!
                val input = command.split(" ")
                when (input[0]) {
                    "optimize" -> {
                        if (input.size < 3) {
                            println("optimize syntax is: optimize <schema> <entity>")
                            continue@loop
                        }
                        optimizeEntity(input[1], input[2])
                    }
                    "preview" -> {  //preview schema entity [count]
                        if (input.size < 3) {
                            println("preview syntax is: preview <schema> <entity> [<count>]")
                            continue@loop
                        }
                        if (input.size == 4) {
                            previewEntity(input[1], input[2], input[3].toLong())
                        } else {
                            previewEntity(input[1], input[2])
                        }
                    }
                    "count" -> {  //preview schema entity [count]
                        if (input.size < 3) {
                            println("count syntax is: count <schema> <entity> [<count>]")
                            continue@loop
                        }
                        countEntity(input[1], input[2])
                    }
                    "query" -> {
                        if(input.size < 5){
                            println("query syntax is: query <schema> <entity> <column> <value>")
                        }
                        queryEntity(input[1], input[2], input[3], input[4])
                    }
                    "list" -> {
                        listEntities()
                    }
                    "quit" -> System.exit(1)
                    else -> printHelp()
                }
                println("fin")
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }

    private fun queryEntity(schema: String, entity: String, col: String, value: String) {
        val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                CottontailGrpc.Query.newBuilder()
                        .setFrom(From(Entity(entity, Schema(schema))))
                        .setProjection(MatchAll())
                        .setWhere(Where(
                                CottontailGrpc.AtomicLiteralBooleanPredicate.newBuilder()
                                        .setAttribute(col)
                                        .setOp(CottontailGrpc.AtomicLiteralBooleanPredicate.Operator.EQUAL)
                                        .addData(CottontailGrpc.Data.newBuilder().setStringData(value).build())
                                        .build()
                        ))
        ).build()
        val query = dqlService.query(qm)
        query.forEach { page ->
            println(page.resultsList)
        }
    }

    private fun printHelp() {
        println("Available commands include help, preview, quit")
    }

    private fun listEntities() {
        println("Listing all Entities")
        ddlService.listSchemas(CottontailGrpc.Empty.getDefaultInstance()).forEach { _schema ->
            println("Entities for Schema ${_schema.name}:")
            ddlService.listEntities(_schema).forEach { _entity ->
                if (_entity.schema.name != _schema.name) {
                    println("Data integrity threat! entity $_entity ist returned when listing entities for schema $_schema")
                }
                println("${_schema.name} - ${_entity.name}")
            }
        }
    }


    private fun countEntity(schema: String, entity: String) {
        println("Counting elements of entity $schema.$entity")
        val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                CottontailGrpc.Query.newBuilder()
                        .setFrom(From(Entity(entity, Schema(schema))))
                        .setProjection(CottontailGrpc.Projection.newBuilder().setOp(CottontailGrpc.Projection.Operation.COUNT).build())

        ).build()
        val query = dqlService.query(qm)
        query.forEach { page ->
            if (!page.resultsList.isEmpty()) {
                println(page.resultsList.first())
            }
        }
    }

    private fun previewEntity(schema: String, entity: String, limit: Long = 10) {
        println("Showing first $limit elements of entity $schema.$entity")
        val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                CottontailGrpc.Query.newBuilder()
                        .setFrom(From(Entity(entity, Schema(schema))))
                        .setProjection(MatchAll())
                        .setLimit(limit)
        ).build()
        val query = dqlService.query(qm)
        println("Previewing $limit elements of $entity")
        query.forEach { page ->
            println(page.resultsList.dropLast(Math.max(0, page.resultsCount - limit).toInt()))
        }
    }

    private fun optimizeEntity(schema: String, entity: String) {
        println("Optimizing entity $schema.$entity")
        ddlService.optimizeEntity(Entity(entity, Schema(schema)))
        println("Done!")
    }
}
