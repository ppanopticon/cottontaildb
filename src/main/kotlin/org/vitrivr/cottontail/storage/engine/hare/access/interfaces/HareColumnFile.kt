package org.vitrivr.cottontail.storage.engine.hare.access.interfaces

import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.basics.Resource
import org.vitrivr.cottontail.storage.engine.hare.disk.HareDiskManager

/**
 * A HARE column file that can hold arbitrary [Value] data.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
interface HareColumnFile<T : Value> : Resource {

    companion object {
        const val SUFFIX = "hare"
    }

    /** The [HareDiskManager] that backs this [HareColumnFile]. */
    val disk: HareDiskManager

    /** The name of this [HareColumnFile]. */
    val name: String

    /** The [Type] describing the column managed by this [HareColumnFile]. */
    val type: Type<T>

    /** Flag indicating whether this [HareColumnFile] supports null entries or not. */
    val nullable: Boolean

    /**
     * Obtains a lock handle on this [HareColumnFile], which will prevent it from being closed.
     * Resources such as [HareColumnWriter], [HareColumnReader] or [HareCursor] are required to
     * obtain such a lock.
     */
    fun obtainLock(): Long

    /**
     * Releases a lock on this [HareColumnFile] that has previously been obtained through [obtainLock].
     *
     * @param handle The lock handle to release.
     */
    fun releaseLock(handle: Long)
}