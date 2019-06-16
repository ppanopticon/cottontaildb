package ch.unibas.dmi.dbis.cottontail.calcite.adapter

import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import org.apache.calcite.schema.Schema
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.impl.AbstractSchema

/**
 * Part of Cottontail DB's Apache Calcite adapter. Exposes Cottontail DB's [Catalogue] class.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class CottontailCatalogue (private val catalogue: Catalogue) : AbstractSchema() {
    /**
     * Returns all the [CottontailSchema] implementations available from this [CottontailCatalogue].
     *
     * @return Map of [CottontailSchema] implementations
     */
    override fun getSubSchemaMap(): Map<String, Schema> = this.catalogue.schemas.map { it to CottontailSchema(this.catalogue.schemaForName(it)) }.toMap()
}