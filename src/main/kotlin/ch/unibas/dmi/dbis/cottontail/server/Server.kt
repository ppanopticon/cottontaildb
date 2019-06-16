package ch.unibas.dmi.dbis.cottontail.server

/**
 * Generic interface that exposes the functionality of a [Server] that can accept and process queries.
 *
 * @version 1.0
 * @author Ralph Gasser
 */
interface Server {
    /** Returns true if this [Server] is currently running, and false otherwise. */
    val isRunning: Boolean

    /** Starts this instance of [Server]. */
    fun start()

    /** Stops this instance of [Server]. */
    fun stop()
}