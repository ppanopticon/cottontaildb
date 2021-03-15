package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.storage.serializers.hare.DoubleVectorValueHareSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.DoubleVectorMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.MapDBSerializer

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [DoubleVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object DoubleVectorValueSerializerFactory : ValueSerializerFactory<DoubleVectorValue> {
    override fun mapdb(size: Int) = DoubleVectorMapDBSerializer(size)
    override fun hare(size: Int) = DoubleVectorValueHareSerializer(size)
}