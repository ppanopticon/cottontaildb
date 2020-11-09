package org.vitrivr.cottontail.database.catalogue.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.model.basics.Name

/**
 * A [Serializer] for [Name.EntityName] objects.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object EntityNameSerializer : Serializer<Name.EntityName> {
    override fun serialize(out: DataOutput2, value: Name.EntityName) {
        out.writeUTF(value.toString())
    }

    override fun deserialize(input: DataInput2, available: Int): Name.EntityName = Name.EntityName(*input.readUTF().split(Name.NAME_COMPONENT_DELIMITER).toTypedArray())
}