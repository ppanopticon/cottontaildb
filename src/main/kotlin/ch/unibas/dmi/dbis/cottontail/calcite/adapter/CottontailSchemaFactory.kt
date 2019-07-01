package ch.unibas.dmi.dbis.cottontail.calcite.adapter

import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.schema.*

/**
 * Part of Cottontail DB's Apache Calcite adapter. Produces instances of [CottontailCatalogue] objects.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal object CottontailSchemaFactory : SchemaFactory {
    /**
     * Returns a [CottontailSchema] or the singleton instance of the [CottontailWarren].
     *
     * @param parentSchema
     * @param name
     * @param operand
     */
    override fun create(parentSchema: SchemaPlus, name: String, operand: MutableMap<String, Any>): Schema = CottontailWarren
}