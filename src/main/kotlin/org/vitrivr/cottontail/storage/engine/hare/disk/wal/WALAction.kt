package org.vitrivr.cottontail.storage.engine.hare.disk.wal

/**
 * Enumeration of write-ahead log actions as used by [WALDiskManager] and [WriteAheadLog].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
enum class WALAction {
    APPEND, UPDATE, FREE
}