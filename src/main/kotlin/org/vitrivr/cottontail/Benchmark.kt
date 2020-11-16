package org.vitrivr.cottontail

import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.database.queries.predicates.KnnPredicateHint
import org.vitrivr.cottontail.grpc.CottonDQLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.values.FloatVectorValue
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime


object Benchmark {
    val channel = ManagedChannelBuilder.forAddress("127.0.0.1", 1865).usePlaintext().build()
    val dqlService = CottonDQLGrpc.newBlockingStub(this.channel)


    val schema = CottontailGrpc.Schema.newBuilder().setName("cineast").build()
    val entity = CottontailGrpc.Entity.newBuilder()
            .setSchema(schema)
            .setName("cineast_metadata")
            .build()



    @ExperimentalTime
    @JvmStatic
    fun main(args: Array<String>) {
        val entities = listOf(
            Pair(CottontailGrpc.Entity.newBuilder().setSchema(schema).setName("features_conceptmasksade20k").build(), 2048),
        )
        val ps = arrayOf(1,2,4,8)
        val k = 100
        Files.newBufferedWriter(Paths.get("benchmark-${System.currentTimeMillis()}.csv"), StandardOpenOption.CREATE_NEW).use {writer ->
            writer.write("entity,repetition,p,d,k,duration (ms)")
            writer.newLine()
            for (e in entities) {
                for (p in ps) {
                    this.runWarmup(e.first, e.second, k, p)
                    val results = this.runBenchmark(e.first, e.second, k, p)
                    results.forEachIndexed { index, duration ->
                        writer.write("${e.first.name},$index,$p,${e.second},$k,$duration")
                        writer.newLine()
                    }
                }
            }
        }
    }

    /**
     * Executes warmup queries.
     */
    fun runWarmup(entity: CottontailGrpc.Entity, dimension: Int, k: Int = 100, p: Int = 1) {
        println("Starting warmup (e = ${entity.name}, d = $dimension, k = $k, p = $p)...")
        repeat(3) {
            val vector = FloatVectorValue.random(dimension).let {
                CottontailGrpc.Vector.newBuilder().setFloatVector(CottontailGrpc.FloatVector.newBuilder().addAllVector(it.data.asIterable()))
            }
            val query = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                    CottontailGrpc.Query.newBuilder()
                            .setFrom(CottontailGrpc.From.newBuilder().setEntity(entity))
                            .setKnn(CottontailGrpc.Knn.newBuilder().addQuery(vector).setDistance(CottontailGrpc.Knn.Distance.L2).setK(k).setAttribute("feature").setHint(CottontailGrpc.KnnHint.newBuilder().setParallelIndexHint(CottontailGrpc.KnnHint.ParallelKnnHint.newBuilder().setMin(p).setMax(p))))
                            .setProjection(CottontailGrpc.Projection.newBuilder().putAttributes("id", "").putAttributes("distance", ""))
            )
            val results = this.dqlService.query(query.build())
            results.forEach {
                it.resultsList.forEach { _ -> }
            }
        }
    }


    /**
     * Executes benchmark queries
     */
    @ExperimentalTime
    fun runBenchmark(entity: CottontailGrpc.Entity, dimension: Int, k: Int = 100, p: Int = 1, r: Int = 10): List<Duration> {
        println("Starting benchmark (e = ${entity.name}, d = $dimension, k = $k, p = $p)...")
        val durations = mutableListOf<Duration>()
        repeat(r) {
            val vector = FloatVectorValue.random(dimension).let {
                CottontailGrpc.Vector.newBuilder().setFloatVector(CottontailGrpc.FloatVector.newBuilder().addAllVector(it.data.asIterable()))
            }
            val query = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                    CottontailGrpc.Query.newBuilder()
                            .setFrom(CottontailGrpc.From.newBuilder().setEntity(entity))
                            .setKnn(CottontailGrpc.Knn.newBuilder().addQuery(vector).setDistance(CottontailGrpc.Knn.Distance.L2).setK(k).setAttribute("feature").setHint(CottontailGrpc.KnnHint.newBuilder().setParallelIndexHint(CottontailGrpc.KnnHint.ParallelKnnHint.newBuilder().setMin(p).setMax(p))))
                            .setProjection(CottontailGrpc.Projection.newBuilder().putAttributes("id", "").putAttributes("distance", ""))
            )

            durations.add(
                measureTime {
                    val results = this.dqlService.query(query.build())
                    results.forEach {
                        it.resultsList.forEach { _ -> }
                    }
                }
            )
        }
        return durations
    }
}




