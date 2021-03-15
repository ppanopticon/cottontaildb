package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.storage.serializers.hare.DoubleValueHareSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.DoubleValueMapDBSerializer

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [DoubleValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object DoubleValueSerializerFactory : ValueSerializerFactory<DoubleValue> {
    override fun mapdb(size: Int) = DoubleValueMapDBSerializer
    override fun hare(size: Int) = DoubleValueHareSerializer
}