package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.Complex64VectorValue
import org.vitrivr.cottontail.storage.serializers.hare.DateValueHareSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.Complex64VectorValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.MapDBSerializer

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [Complex64VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object Complex64VectorValueSerializerFactory : ValueSerializerFactory<Complex64VectorValue> {
    override fun mapdb(size: Int): MapDBSerializer<Complex64VectorValue> = Complex64VectorValueMapDBSerializer(size)
    override fun hare(size: Int) = TODO()
}