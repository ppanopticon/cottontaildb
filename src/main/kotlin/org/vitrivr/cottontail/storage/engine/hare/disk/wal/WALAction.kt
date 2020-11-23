package org.vitrivr.cottontail.storage.engine.hare.disk.wal

/**
 * Enumeration of undo log actions as used by [WALHareDiskManager]'s [WriteAheadLog].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class WALAction {
    /** Snapshot of a page prior to changing it, logged in the [WriteAheadLog]. */
    UNDO_SNAPSHOT,
}