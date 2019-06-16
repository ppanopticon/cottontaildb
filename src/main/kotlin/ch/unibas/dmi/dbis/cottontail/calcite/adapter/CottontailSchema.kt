package ch.unibas.dmi.dbis.cottontail.calcite.adapter

import ch.unibas.dmi.dbis.cottontail.database.schema.Schema

import org.apache.calcite.schema.Table
import org.apache.calcite.schema.impl.AbstractSchema

/**
 * Part of Cottontail DB's Apache Calcite adapter. Exposes Cottontail DB's [Schema] class.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class CottontailSchema(val schema: Schema) : AbstractSchema() {
    /**
     * Returns the list of [CottontailTable]'s for this [CottontailSchema].
     *
     * @return Map that maps the available [CottontailTable]s
     */
    override fun getTableMap(): Map<String, Table> = this.schema.entities.map {
        it to CottontailTable(this.schema.entityForName(it))
    }.toMap()
}