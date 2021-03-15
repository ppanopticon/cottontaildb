package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.FloatValue
import org.vitrivr.cottontail.storage.serializers.hare.FloatValueHareSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.FloatValueMapDBSerializer

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [FloatValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object FloatValueSerializerFactory : ValueSerializerFactory<FloatValue> {
    override fun mapdb(size: Int) = FloatValueMapDBSerializer
    override fun hare(size: Int) = FloatValueHareSerializer
}