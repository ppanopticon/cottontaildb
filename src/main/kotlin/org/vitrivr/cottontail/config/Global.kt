package org.vitrivr.cottontail.config

object Global {
    /** The number of logical threads available to Cottontail DB. */
    val LOGICAL_THREADS = Runtime.getRuntime().availableProcessors()
}