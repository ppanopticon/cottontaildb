package ch.unibas.dmi.dbis.cottontail.calcite.adapter

import ch.unibas.dmi.dbis.cottontail.Cottontail

import org.apache.calcite.schema.Schema
import org.apache.calcite.schema.SchemaFactory
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.impl.AbstractSchema

/**
 * Part of Cottontail DB's Apache Calcite adapter. Produces instances of [CottontailCatalogue] objects.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal object CottontailCatalogueFactory : SchemaFactory {
    /**
     * Returns singleton instance of [CottontailCatalogue].
     *
     * @param parentSchema
     * @param name
     * @param operand
     */
    override fun create(parentSchema: SchemaPlus, name: String, operand: MutableMap<String, Any>): Schema = object : AbstractSchema() {
        override fun getSubSchemaMap(): Map<String, Schema> = mapOf("warren" to CottontailCatalogue(Cottontail.CATALOGUE!!))
    }
}