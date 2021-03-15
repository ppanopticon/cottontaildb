package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.IntVectorValue
import org.vitrivr.cottontail.storage.serializers.mapdb.IntVectorValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.MapDBSerializer

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [IntVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object IntVectorValueSerializerFactory : ValueSerializerFactory<IntVectorValue> {
    override fun mapdb(size: Int): MapDBSerializer<IntVectorValue> = IntVectorValueMapDBSerializer(size)
    override fun hare(size: Int) = TODO()
}