package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.Complex32VectorValue
import org.vitrivr.cottontail.storage.serializers.mapdb.Complex32VectorValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.MapDBSerializer

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [Complex32VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object Complex32VectorValueSerializerFactory : ValueSerializerFactory<Complex32VectorValue> {
    override fun mapdb(size: Int): MapDBSerializer<Complex32VectorValue> = Complex32VectorValueMapDBSerializer(size)
    override fun hare(size: Int) = TODO()
}