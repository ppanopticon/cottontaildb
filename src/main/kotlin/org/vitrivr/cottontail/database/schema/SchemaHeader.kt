package org.vitrivr.cottontail.database.schema

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import java.util.*

/**
 * The header of the [DefaultSchema]. Contains information regarding its content!
 *
 * @see DefaultSchema
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
data class SchemaHeader(
    val name: String,
    val created: Long = System.currentTimeMillis(),
    val modified: Long = System.currentTimeMillis(),
    val entities: List<EntityRef> = LinkedList()
) {

    companion object Serializer : org.mapdb.Serializer<SchemaHeader> {
        override fun serialize(out: DataOutput2, value: SchemaHeader) {
            out.packInt(DBOVersion.V2_0.ordinal)
            out.writeUTF(value.name)
            out.writeLong(value.created)
            out.writeLong(value.modified)
            out.packInt(value.entities.size)
            value.entities.forEach { EntityRef.serialize(out, it) }
        }

        override fun deserialize(input: DataInput2, available: Int): SchemaHeader {
            val version = DBOVersion.values()[input.unpackInt()]
            if (version != DBOVersion.V2_0)
                throw DatabaseException.VersionMismatchException(version, DBOVersion.V2_0)
            return SchemaHeader(
                input.readUTF(),
                input.readLong(),
                input.readLong(),
                (0 until input.unpackInt()).map { EntityRef.deserialize(input, available) }
            )
        }
    }

    /**
     * Reference pointing to an entity.
     */
    data class EntityRef(val name: String) {
        companion object Serializer : org.mapdb.Serializer<EntityRef> {
            override fun serialize(out: DataOutput2, value: EntityRef) {
                out.writeUTF(value.name)
            }

            override fun deserialize(input: DataInput2, available: Int): EntityRef = EntityRef(
                input.readUTF()
            )
        }
    }
}