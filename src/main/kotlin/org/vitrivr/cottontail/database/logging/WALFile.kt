package org.vitrivr.cottontail.database.logging

import org.vitrivr.cottontail.database.logging.operations.Operation
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/**
 * A Write Ahead Log file.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class WALFile(val path: Path) {

    /** The [FileChannel] used to read / write to this [WALFile]. */
    private val channel = FileChannel.open(this.path, StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND, StandardOpenOption.SYNC)

    /**
     * Logs an [Operation] to this [WALFile] and writes it to disk.
     */
    @Synchronized
    fun log(operation: Operation): Long {
        return this.channel.position()
    }
}