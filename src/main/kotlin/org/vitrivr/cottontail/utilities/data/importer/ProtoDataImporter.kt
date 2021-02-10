package org.vitrivr.cottontail.utilities.data.importer

import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.utilities.data.Format
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/**
 * A [DataImporter] implementation that can be used to read a [Format.PROTO] file containing an list of entries.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ProtoDataImporter(override val path: Path) : DataImporter {

    /** The [Format] handled by this [DataImporter]. */
    override val format: Format = Format.PROTO

    /** Indicator whether this [DataImporter] has been closed. */
    override var closed: Boolean = false
        private set

    /** Internal [InputStream] */
    private val input = Files.newInputStream(this.path, StandardOpenOption.READ)

    /** Next element that can be returned by this [ProtoDataImporter]. */
    protected var next: CottontailGrpc.InsertMessage? = null

    /**
     * Returns the next [CottontailGrpc.InsertMessage.Builder] from the data.
     *
     * @return [CottontailGrpc.InsertMessage.Builder]
     */
    override fun next(): CottontailGrpc.InsertMessage.Builder {
        val ret = this.next ?: throw NoSuchElementException("")
        this.next = null
        return ret.toBuilder()
    }

    /**
     * Checks if there is another entry and returns true if so, and false otherwise.
     *
     * @return True if there is another entry, false otherwise.
     */
    override fun hasNext(): Boolean {
        return try {
            this.next = CottontailGrpc.InsertMessage.parseDelimitedFrom(this.input)
            this.next != null
        } catch (e: Throwable) {
            false
        }
    }

    /**
     * Closes this [ProtoDataImporter]
     */
    override fun close() {
        if (!this.closed) {
            this.input.close()
            this.closed = true
        }
    }
}