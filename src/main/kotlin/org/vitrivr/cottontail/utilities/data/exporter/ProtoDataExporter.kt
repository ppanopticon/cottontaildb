package org.vitrivr.cottontail.utilities.data.exporter

import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.utilities.data.Format
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/**
 * A [DataExporter] implementation that can be used to write a [Format.PROTO] files containing a list of entries.
 *
 * Data produced by the [ProtoDataExporter] can be used as input for the [ProtoDataImporter].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ProtoDataExporter(override val path: Path) : DataExporter {
    /** The [Format] handled by this [DataImporter]. */
    override val format: Format = Format.PROTO

    /** Indicator whether this [DataImporter] has been closed. */
    override var closed: Boolean = false
        private set

    /** Internal [OutputStream] */
    private val output =
        Files.newOutputStream(this.path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)

    /**
     * Offers a new [CottontailGrpc.QueryResponseMessage] to this [ProtoDataExporter], which will
     * be appended to the output stream.
     *
     * @param message The [CottontailGrpc.QueryResponseMessage] to append.
     */
    override fun offer(message: CottontailGrpc.QueryResponseMessage) {
        for (tuple in message.tuplesList) {
            val insert = CottontailGrpc.InsertMessage.newBuilder()
            tuple.dataList.zip(message.columnsList).forEach { e ->
                insert.addInserts(
                    CottontailGrpc.InsertMessage.InsertElement.newBuilder().setValue(e.first)
                        .setColumn(e.second)
                )
            }
            insert.build().writeDelimitedTo(this.output)
        }
        this.output.flush()
    }

    /**
     * Closes this [ProtoDataExporter].
     */
    override fun close() {
        if (!this.closed) {
            this.output.close()
            this.closed = true
        }
    }
}