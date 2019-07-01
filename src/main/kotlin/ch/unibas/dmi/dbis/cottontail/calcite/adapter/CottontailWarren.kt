package ch.unibas.dmi.dbis.cottontail.calcite.adapter

import ch.unibas.dmi.dbis.cottontail.Cottontail
import ch.unibas.dmi.dbis.cottontail.calcite.knn.UnaryScanningKnn
import ch.unibas.dmi.dbis.cottontail.calcite.knn.WeightedUnaryScanningKnn
import com.google.common.collect.ImmutableMultimap
import com.google.common.collect.Multimap
import org.apache.calcite.schema.Function
import org.apache.calcite.schema.Schema
import org.apache.calcite.schema.impl.AbstractSchema
import org.apache.calcite.schema.impl.TableFunctionImpl

/**
 *
 */
object CottontailWarren : AbstractSchema() {
    /** Map of the available functions (Calcite UDFs). */
    val FUNCTIONS: Multimap<String, Function> = ImmutableMultimap.builder<String, Function>()
            .put("knn", TableFunctionImpl.create(UnaryScanningKnn::class.java, "evaluate"))
            .put("knn", TableFunctionImpl.create(WeightedUnaryScanningKnn::class.java, "evaluate"))
            .build()
    /**
     * Returns a list of all [CottontailSchema]s available in this [CottontailWarren].
     *
     * @return Map of available [CottontailSchema]s.
     */
    override fun getSubSchemaMap(): Map<String, Schema> = mapOf(*Cottontail.CATALOGUE!!.schemas.map { it to  CottontailSchema(it, Cottontail.CATALOGUE!!)}.toTypedArray())

    /**
     * Returns a list of [Function]s available for this [CottontailSchema].
     *
     * @return Map of available [Function]s.
     */
    override fun getFunctionMultimap(): Multimap<String, Function> = FUNCTIONS
}