package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.FloatVectorValue
import org.vitrivr.cottontail.storage.serializers.hare.FloatVectorValueHareSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.FloatVectorMapDBValueSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.MapDBSerializer

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [FloatVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object FloatVectorValueSerializerFactory : ValueSerializerFactory<FloatVectorValue> {
    override fun mapdb(size: Int) = FloatVectorMapDBValueSerializer(size)
    override fun hare(size: Int) = FloatVectorValueHareSerializer(size)
}