package org.vitrivr.cottontail.cli.entity

import io.grpc.StatusException
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.server.grpc.helper.proto
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to optimize an entity (i.e. rebuild all its index structures).
 *
 * @author Loris Sauter
 * @version 1.0.1
 */
@ExperimentalTime
class OptimizeEntityCommand(private val ddlStub: DDLGrpc.DDLBlockingStub) : AbstractEntityCommand(name = "optimize", help = "Optimizes the specified entity, i.e., rebuilds its index structures.") {
    override fun exec() {
        println("Optimizing entity ${this.entityName}. This might take a while...")
        try {
            val timedTable = measureTimedValue {
                TabulationUtilities.tabulate(this.ddlStub.optimizeEntity(CottontailGrpc.OptimizeEntityMessage.newBuilder().setEntity(this.entityName.proto()).build()))
            }
            println("Successfully optimized entity ${this.entityName} (took ${timedTable.duration}).")
            print(timedTable.value)
        } catch (e: StatusException) {
            println("Failed to optimize entity ${this.entityName}: ${e.message}")
        }
    }
}