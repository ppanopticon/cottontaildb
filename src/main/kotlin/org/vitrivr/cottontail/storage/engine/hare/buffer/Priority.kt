package org.vitrivr.cottontail.storage.engine.hare.buffer

/**
 * [Priority] of a [PageRef]. The higher, the more important it is and the more like to be retained in the [BufferPool].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
enum class Priority {
    UNINIT, LOW, DEFAULT, HIGH;
}