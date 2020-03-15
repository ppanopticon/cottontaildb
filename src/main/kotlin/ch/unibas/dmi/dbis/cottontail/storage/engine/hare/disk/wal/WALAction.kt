package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.wal

/**
 * Enumeration of write-ahead log actions as used by HARE.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
enum class WALAction {
    UPDATE, APPEND, FREE
}