package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

/**
 * Config for Cottontail DB's task execution engine.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
@Serializable
data class ExecutionConfig(
    val coreThreads: Int = (Runtime.getRuntime().availableProcessors() / 2),
    val maxThreads: Int = Runtime.getRuntime().availableProcessors(),
    val keepAliveMs: Long = 1000L,
    val queueSize: Int = 100,
    val transactionTableSize: Int = 100,
    val transactionHistorySize: Int = 500
) {

    /**
     *  Creates and returns a new [ThreadPoolExecutor] using this [ExecutionConfig].
     *
     *  @return [ThreadPoolExecutor]
     */
    fun newExecutor() = ThreadPoolExecutor(
        this.coreThreads,
        this.maxThreads,
        this.keepAliveMs,
        TimeUnit.MILLISECONDS,
        ArrayBlockingQueue(this.queueSize)
    )
}