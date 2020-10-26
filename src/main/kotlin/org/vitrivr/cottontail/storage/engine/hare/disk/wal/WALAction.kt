package org.vitrivr.cottontail.storage.engine.hare.disk.wal

/**
 * Enumeration of write-ahead log actions as used by [WALDiskManager] and [WriteAheadLog].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class WALAction {
    /** New page was allocated appended to the end of the [WALDiskManager]. */
    ALLOCATE_APPEND,

    /** New page was allocated by reusing an existing page of the [WALDiskManager]. */
    ALLOCATE_REUSE,

    /** An existing page was updated. */
    UPDATE,

    /** A page was freed by adding it to the list of reusable pages. */
    FREE
}