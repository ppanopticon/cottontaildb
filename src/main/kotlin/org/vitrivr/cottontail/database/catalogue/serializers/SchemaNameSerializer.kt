package org.vitrivr.cottontail.database.catalogue.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.model.basics.Name

/**
 * A [Serializer] for [Name.SchemaName] objects.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object SchemaNameSerializer : Serializer<Name.SchemaName> {
    override fun serialize(out: DataOutput2, value: Name.SchemaName) {
        out.writeUTF(value.toString())
    }

    override fun deserialize(input: DataInput2, available: Int): Name.SchemaName = Name.SchemaName(*input.readUTF().split(Name.NAME_COMPONENT_DELIMITER).toTypedArray())
}