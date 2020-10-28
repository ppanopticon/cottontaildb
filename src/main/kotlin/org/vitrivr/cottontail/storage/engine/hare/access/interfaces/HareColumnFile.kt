package org.vitrivr.cottontail.storage.engine.hare.access.interfaces

import org.vitrivr.cottontail.model.values.types.Value

/**
 * A HARE column file that can hold arbitrary [Value] data.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface HareColumnFile<T: Value> : AutoCloseable {

    /**
     * Creates and returns a new [HareColumnReader] for this [HareColumnFile].
     *
     * @return The [HareColumnReader]
     */
    fun newReader(): HareColumnReader<T>

    /**
     * Creates and returns a new [HareColumnWriter] for this [HareColumnFile].
     *
     * @return The [HareColumnWriter]
     */
    fun newWriter(): HareColumnWriter<T>

    /**
     * Creates and returns a new [HareCursor] for this [HareColumnFile].
     *
     * @return The [HareCursor]
     */
    fun newCursor(): HareCursor<T>
}