package org.vitrivr.cottontail.utilities.data

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.utilities.data.exporter.DataExporter
import org.vitrivr.cottontail.utilities.data.exporter.JsonDataExporter
import org.vitrivr.cottontail.utilities.data.exporter.ProtoDataExporter
import org.vitrivr.cottontail.utilities.data.importer.DataImporter
import org.vitrivr.cottontail.utilities.data.importer.JsonDataImporter
import org.vitrivr.cottontail.utilities.data.importer.ProtoDataImporter
import java.nio.file.Path

/**
 * Data import and export format used and supported by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
enum class Format(val suffix: String) {
    /** ProtocolBuffer format. */
    PROTO("proto"),

    /** JSON format. */
    JSON("json"),

    /** CSV format. */
    CSV("csv");

    /**
     * Creates and returns a new [DataImporter] for this [Format].
     *
     * @param file [Path] to input file.
     * @param schema Optional list of [ColumnDef]s that should be imported (not required).
     * @return [DataImporter]
     */
    fun newImporter(file: Path, schema: Array<ColumnDef<*>>? = null): DataImporter = when (this) {
        PROTO -> ProtoDataImporter(file)
        JSON -> JsonDataImporter(
            file,
            schema
                ?: throw IllegalArgumentException("Schema is required to create an instance of JsonDataImporter.")
        )
        CSV -> TODO()
    }

    /**
     * Creates and returns a new [DataExporter] for this [Format].
     *
     * @param file [Path] to input file.
     * @return [DataExporter]
     */
    fun newExporter(file: Path): DataExporter = when (this) {
        PROTO -> ProtoDataExporter(file)
        JSON -> JsonDataExporter(file)
        CSV -> TODO()
    }
}