package org.vitrivr.cottontail.database.catalogue.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.model.basics.Name

/**
 * A [Serializer] for [Name.IndexName] objects.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object IndexNameSerializer : Serializer<Name.IndexName> {
    override fun serialize(out: DataOutput2, value: Name.IndexName) {
        out.writeUTF(value.toString())
    }

    override fun deserialize(input: DataInput2, available: Int): Name.IndexName = Name.IndexName(*input.readUTF().split(Name.NAME_COMPONENT_DELIMITER).toTypedArray())
}