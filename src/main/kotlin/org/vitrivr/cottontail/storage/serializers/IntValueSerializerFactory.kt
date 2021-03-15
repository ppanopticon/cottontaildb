package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.storage.serializers.hare.IntValueHareSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.IntValueMapDBSerializer

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [IntValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object IntValueSerializerFactory : ValueSerializerFactory<IntValue> {
    override fun mapdb(size: Int) = IntValueMapDBSerializer
    override fun hare(size: Int) = IntValueHareSerializer
}