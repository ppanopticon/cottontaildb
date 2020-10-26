package org.vitrivr.cottontail.storage.engine.hare.basics


/**
 * Collection of constants used with [Page] data structures.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object PageConstants {

    /** Constant used to identify a [Page] that has been freed. Used internally by [org.vitrivr.cottontail.storage.engine.hare.disk.DiskManager] */
    const val PAGE_TYPE_FREED = Long.MIN_VALUE

    /**
     * Constant used to identify a [Page] that has not been initialized as [AbstractPageView].
     *
     * This is a sensible choice, since the first four bytes of all new [Page] are always 0.
     */
    const val PAGE_TYPE_UNINITIALIZED = 0

    /** Constant used to identify a [Page] intended for use with a [SlottedPageView]. */
    const val PAGE_TYPE_SLOTTED = 128

    /** Constant used to identify a [Page] intended for use with a [DirectoryPageView]. */
    const val PAGE_TYPE_DIRECTORY = 129

    /** Constant used to identify a [Page] used as a header for a [org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnFile]. */
    const val PAGE_TYPE_HEADER_FIXED_COLUMN = 512

    /** Constant used to identify a [Page] used as a header for a [org.vitrivr.cottontail.storage.engine.hare.access.column.variable.VariableHareColumnFile]. */
    const val PAGE_TYPE_HEADER_VARIABLE_COLUMN = 513
}