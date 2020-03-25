package ch.unibas.dmi.dbis.cottontail

import ch.unibas.dmi.dbis.cottontail.grpc.CottonDDLGrpc
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDMLGrpc
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDQLGrpc
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc

import io.grpc.ManagedChannelBuilder


object Playground {


    val channel = ManagedChannelBuilder.forAddress("127.0.0.1", 1865).usePlaintext().build()

    val dqlService =  CottonDQLGrpc.newBlockingStub(channel)
    val ddlService =  CottonDDLGrpc.newBlockingStub(channel)
    val dmlService =  CottonDMLGrpc.newBlockingStub(channel)

    val schema = CottontailGrpc.Schema.newBuilder().setName("test").build()
    val entity = CottontailGrpc.Entity.newBuilder()
            .setSchema(schema)
            .setName("surf")
            .build()


    @JvmStatic
    fun main(args: Array<String>) {



        this.ddlService.createIndex(
                CottontailGrpc.CreateIndexMessage.newBuilder().addColumns("feature").setIndex(
                    CottontailGrpc.Index.newBuilder()
                            .setEntity(CottontailGrpc.Entity.newBuilder().setName("features_audiotranscription").setSchema(CottontailGrpc.Schema.newBuilder().setName("cineast").build()))
                            .setType(CottontailGrpc.Index.IndexType.LUCENE)
                            .setName("feature_index")
                            .build()
                ).build()
        )

    }
}