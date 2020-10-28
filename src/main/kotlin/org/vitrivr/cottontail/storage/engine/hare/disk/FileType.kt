package org.vitrivr.cottontail.storage.engine.hare.disk

/**
 * Specifies the file types for the HARE format.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class FileType {
    PAGE, /* A HARE page file. */
    WAL /* A HARE WAL file. */
}