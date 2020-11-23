package org.vitrivr.cottontail.storage.engine.hare.disk.wal

/**
 * The state of a [WriteAheadLog].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class WALState(val isSealed: Boolean) {
    LOGGING(false),

    /** Indicates, that the [WriteAheadLog] is ready to receive log entries. */
    ABORTED(true),

    /** Indicates, that an ABORT has been recorded for the [WriteAheadLog]. Seals the [WriteAheadLog]. */
    COMMITTED(true)
    /** Indicates, that a COMMIT has been recorded for the [WriteAheadLog]. Seals the [WriteAheadLog]. */
}