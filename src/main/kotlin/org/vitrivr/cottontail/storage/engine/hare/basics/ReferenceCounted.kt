package org.vitrivr.cottontail.storage.engine.hare.basics

/**
 *
 */
interface ReferenceCounted {
    companion object {
        const val REF_COUNT_DISPOSED = -1865
    }

    /** Reference count for this [ReferenceCounted] object instance. */
    val refCount: Int

    /** Returns true, if this [ReferenceCounted] has been disposed and its use is therefore unsafe. */
    val disposed: Boolean
        get() = this.refCount == REF_COUNT_DISPOSED

    /**
     * Retains this [ReferenceCounted] object thus increasing its reference count by one.
     *
     * @throws IllegalStateException If this [refCount] has dropped to [REF_COUNT_DISPOSED].
     */
    fun retain(): ReferenceCounted

    /**
     * Releases this [ReferenceCounted] object thus decreasing its reference count by one.
     *
     * @throws IllegalStateException If this [refCount] has dropped to [REF_COUNT_DISPOSED] or als already 0.
     */
    fun release()
}