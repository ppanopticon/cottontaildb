package org.vitrivr.cottontail.utilities.data.exporter

import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.utilities.data.Format
import java.nio.file.Path

/**
 * A class that can be used for exporting  data out of Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface DataExporter : AutoCloseable {
    /** The [Path] to the import file. */
    val path: Path

    /** The [Format] handled by this [DataExporter]. */
    val format: Format

    /** Indicator whether this [DataExporter] has been closed. */
    val closed: Boolean

    /**
     * Offers a [CottontailGrpc.QueryResponseMessage] for export by this [DataExporter].
     *
     * Due to the nature of how Cottontail DB returns queries, individual tuples are written-out
     * to disk in batches.
     */
    fun offer(message: CottontailGrpc.QueryResponseMessage)
}