package ch.unibas.dmi.dbis.cottontail.calcite.adapter

import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import com.google.common.collect.ImmutableMap
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.Schemas

import org.apache.calcite.schema.Table
import org.apache.calcite.schema.impl.AbstractSchema
import org.apache.calcite.tools.Frameworks
import java.awt.Frame

/**
 * Part of Cottontail DB's Apache Calcite adapter. Exposes Cottontail DB's [Schema] class.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class CottontailSchema(name: String, catalogue: Catalogue) : AbstractSchema() {

    /** Reference to the [Schema] that is used for data access. */
    private val source = catalogue.schemaForName(name)

    /**
     * Returns the list of [CottontailTable]'s for this [CottontailSchema].
     *
     * @return Map that maps the available [CottontailTable]s
     */
    override fun getTableMap(): Map<String, Table> {
        val builder = ImmutableMap.builder<String,Table>()
        this.source.entities.forEach { builder.put(it, CottontailTable(it, this.source)) }
        return builder.build()
    }

    override fun getExpression(parentSchema: SchemaPlus, name: String): Expression {
        return Schemas.subSchemaExpression(parentSchema, name, javaClass)
    }

}