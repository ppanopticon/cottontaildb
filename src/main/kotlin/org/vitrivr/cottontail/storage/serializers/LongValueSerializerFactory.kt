package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.storage.serializers.hare.LongValueHareSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.LongValueMapDBSerializer

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [LongValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object LongValueSerializerFactory : ValueSerializerFactory<LongValue> {
    override fun mapdb(size: Int) = LongValueMapDBSerializer
    override fun hare(size: Int) = LongValueHareSerializer
}