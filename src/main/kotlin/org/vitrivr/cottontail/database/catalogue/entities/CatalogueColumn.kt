package org.vitrivr.cottontail.database.catalogue.entities

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.column.ColumnDriver
import org.vitrivr.cottontail.database.column.ColumnType
import java.nio.file.Path
import java.nio.file.Paths

/**
 * The entry that describes a column in Cottontail DB [Catalogue][org.vitrivr.cottontail.database.catalogue.Catalogue].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class CatalogueColumn(val path: Path, val type: ColumnType<*>, val logicalSize: Int, val nullable: Boolean, val driver: ColumnDriver) {
    /**
     * [Serializer] implementation for [CatalogueEntity].
     */
    object Serializer : org.mapdb.Serializer<CatalogueColumn> {
        override fun serialize(out: DataOutput2, value: CatalogueColumn) {
            out.writeUTF(value.path.toString())
            out.packInt(value.type.ordinal)
            out.packInt(value.logicalSize)
            out.writeBoolean(value.nullable)
            out.packInt(value.driver.ordinal)
        }

        override fun deserialize(input: DataInput2, available: Int): CatalogueColumn = CatalogueColumn(
                Paths.get(input.readUTF()),
                ColumnType.forOrdinal(input.unpackInt()),
                input.unpackInt(),
                input.readBoolean(),
                ColumnDriver.values()[input.unpackInt()]
        )
    }
}