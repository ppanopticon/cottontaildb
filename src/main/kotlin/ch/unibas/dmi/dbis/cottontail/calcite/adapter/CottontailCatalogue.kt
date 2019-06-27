package ch.unibas.dmi.dbis.cottontail.calcite.adapter

import ch.unibas.dmi.dbis.cottontail.calcite.knn.UnaryScanningKnn
import ch.unibas.dmi.dbis.cottontail.calcite.knn.WeightedUnaryScanningKnn
import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableMultimap
import com.google.common.collect.Multimap
import org.apache.calcite.schema.Function
import org.apache.calcite.schema.Schema
import org.apache.calcite.schema.Table
import org.apache.calcite.schema.impl.AbstractSchema
import org.apache.calcite.schema.impl.TableFunctionImpl

/**
 * Part of Cottontail DB's Apache Calcite adapter. Exposes Cottontail DB's [Catalogue] class.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class CottontailCatalogue (private val catalogue: Catalogue) : AbstractSchema() {
    /**
     * Initialises the available functions (Calcite UDFs).
     */
    companion object {
        val FUNCTIONS: Multimap<String,Function> = ImmutableMultimap.builder<String,Function>()
                .put("knn", TableFunctionImpl.create(UnaryScanningKnn::class.java, "evaluate"))
                .put("knn", TableFunctionImpl.create(WeightedUnaryScanningKnn::class.java, "evaluate"))
                .build()
    }

    /**
     * Returns all the [CottontailSchema] implementations available from this [CottontailCatalogue].
     *
     * @return Map of [CottontailSchema] implementations.
     */
    override fun getSubSchemaMap(): Map<String, Schema> {
        val builder = ImmutableMap.builder<String, Schema>()
        this.catalogue.schemas.forEach { builder.put(it, CottontailSchema(it, catalogue)) }
        return builder.build()
    }

    /**
     * Returns all the [Function] implementations available from this [CottontailCatalogue].
     *
     * @return Map of [Function] implementations.
     */
    override fun getFunctionMultimap(): Multimap<String, Function> = FUNCTIONS
}