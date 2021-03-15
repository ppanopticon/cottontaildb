package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.Complex32Value
import org.vitrivr.cottontail.storage.serializers.hare.Complex32ValueHareSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.Complex32ValueMapDBSerializer

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [Complex32Value]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object Complex32ValueSerializerFactory : ValueSerializerFactory<Complex32Value> {
    override fun mapdb(size: Int) = Complex32ValueMapDBSerializer
    override fun hare(size: Int) = Complex32ValueHareSerializer
}