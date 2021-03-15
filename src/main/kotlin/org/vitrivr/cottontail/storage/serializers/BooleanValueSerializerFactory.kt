package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.BooleanValue
import org.vitrivr.cottontail.storage.serializers.hare.BooleanValueHareSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.BooleanValueMapDBSerializer

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [BooleanValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object BooleanValueSerializerFactory : ValueSerializerFactory<BooleanValue> {
    override fun mapdb(size: Int) = BooleanValueMapDBSerializer
    override fun hare(size: Int) = BooleanValueHareSerializer
}