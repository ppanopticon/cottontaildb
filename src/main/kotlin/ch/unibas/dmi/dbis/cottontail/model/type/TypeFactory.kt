package ch.unibas.dmi.dbis.cottontail.model.type

/**
 * A factory object used to generate [Type] references from type names.
 *
 * @see Type
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object TypeFactory {
    /**
     * Returns the [Type] for the provided name.
     *
     * @param name For which to lookup the [Type].
     */
    fun forName(name: String): Type<*> = when(name.toUpperCase()) {
        "BOOLEAN" -> BooleanType
        "BYTE" -> ByteType
        "SHORT" -> ShortType
        "INTEGER" -> IntType
        "LONG" -> LongType
        "FLOAT" -> FloatType
        "DOUBLE" -> DoubleType
        "STRING" -> StringType
        "INT_VEC" -> IntArrayType
        "LONG_VEC" -> LongArrayType
        "FLOAT_VEC" -> FloatArrayType
        "DOUBLE_VEC" -> DoubleArrayType
        "BOOLEAN_VEC" -> BooleanArrayType
        else -> throw java.lang.IllegalArgumentException("The column type $name does not exists!")
    }
}