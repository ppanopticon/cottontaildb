package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.ShortValue
import org.vitrivr.cottontail.storage.serializers.hare.HareSerializer
import org.vitrivr.cottontail.storage.serializers.hare.ShortValueHareSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.ShortValueMapDBSerializer

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [ShortValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object ShortValueSerializerFactory : ValueSerializerFactory<ShortValue> {
    override fun mapdb(size: Int) = ShortValueMapDBSerializer
    override fun hare(size: Int) = ShortValueHareSerializer
}