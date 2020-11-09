package org.vitrivr.cottontail.database.catalogue.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.model.basics.Name

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object ColumnNameSerializer : Serializer<Name.ColumnName> {
    override fun serialize(out: DataOutput2, value: Name.ColumnName) {
        out.writeUTF(value.toString())
    }

    override fun deserialize(input: DataInput2, available: Int): Name.ColumnName = Name.ColumnName(*input.readUTF().split(Name.NAME_COMPONENT_DELIMITER).toTypedArray())
}