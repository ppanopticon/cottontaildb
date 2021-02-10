package org.vitrivr.cottontail.utilities.data.importer

import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.utilities.data.Format
import java.nio.file.Path

/**
 * A class that can be used for parsing and importing external data into Cottontail DB. Basically
 * and extended [Iterator] for [CottontailGrpc.InsertMessage]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface DataImporter : Iterator<CottontailGrpc.InsertMessage.Builder>, AutoCloseable {
    /** The [Path] to the import file. */
    val path: Path

    /** The [Format] handled by this [DataImporter]. */
    val format: Format

    /** Indicator whether this [DataImporter] has been closed. */
    val closed: Boolean
}