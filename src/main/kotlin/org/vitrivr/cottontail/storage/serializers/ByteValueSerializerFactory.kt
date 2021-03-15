package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.ByteValue
import org.vitrivr.cottontail.storage.serializers.hare.ByteValueHareSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.ByteValueMapDBSerializer

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [ByteValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object ByteValueSerializerFactory : ValueSerializerFactory<ByteValue> {
    override fun mapdb(size: Int) = ByteValueMapDBSerializer
    override fun hare(size: Int) = ByteValueHareSerializer
}