package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics


/**
 * [Releasable]s implement a simple reference counting or pin counting system that acts as a hint to
 * some resource management unit.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface Releasable {
    companion object {
        const val PIN_COUNT_DISPOSED = -1
    }

    /** Number of pins for this [Releasable]. */
    val pinCount: Int

    /** Retains this [Releasable], increasing its [Releasable.pinCount] by one. */
    fun retain(): Releasable

    /**
     * Retains this [Releasable], decreasing its [Releasable.pinCount] by one. Once a
     * [Releasable]'s [Releasable.pinCount] drops to zero, it may be disposed or recycled
     * at the discretion of the memory management unit.
     */
    fun release(): Releasable
}