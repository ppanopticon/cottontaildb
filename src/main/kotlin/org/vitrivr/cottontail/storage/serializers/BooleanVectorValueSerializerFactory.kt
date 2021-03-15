package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.BooleanVectorValue
import org.vitrivr.cottontail.storage.serializers.mapdb.BooleanVectorValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.MapDBSerializer

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [BooleanVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object BooleanVectorValueSerializerFactory : ValueSerializerFactory<BooleanVectorValue> {
    override fun mapdb(size: Int): MapDBSerializer<BooleanVectorValue> = BooleanVectorValueMapDBSerializer(size)
    override fun hare(size: Int) = TODO()
}