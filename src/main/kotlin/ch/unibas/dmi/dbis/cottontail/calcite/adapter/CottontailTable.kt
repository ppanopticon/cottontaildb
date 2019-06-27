package ch.unibas.dmi.dbis.cottontail.calcite.adapter

import ch.unibas.dmi.dbis.cottontail.calcite.expressions.CottontailTableScan
import ch.unibas.dmi.dbis.cottontail.calcite.enumerators.CottontailEntityEnumerator
import ch.unibas.dmi.dbis.cottontail.calcite.utilities.Entry
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.model.type.*
import ch.unibas.dmi.dbis.cottontail.model.values.Value
import ch.unibas.dmi.dbis.cottontail.utilities.name.last

import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.schema.*
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.schema.Schemas
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.*

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.rel.*
import org.apache.calcite.util.ImmutableBitSet
import java.lang.reflect.Type

/**
 * Part of Cottontail DB's Apache Calcite adapter. Exposes Cottontail DB's [Entity] class.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class CottontailTable(name: String, schema: Schema) : AbstractTable(), TranslatableTable, QueryableTable {

    /** Reference to the Cottontail DB [Entity] that acts as data source. */
    val source = schema.entityForName(name)

    /**
     * Returns the type of the relations (rows) returned by this instance of [CottontailTable].
     *
     * @param typeFactory The [RelDataTypeFactory] used to construct the type.
     */
    override fun getRowType(typeFactory: RelDataTypeFactory): RelDataType = typeFactory.createStructType(this.source.allColumns().map {it.toSQLType(typeFactory)})

    /**
     * Returns some [Statistics] that describes the [Entity] backing this [CottontailTable].
     *
     * @return [Statistics] regarding the [Entity] that underpins this [CottontailTable].
     */
    override fun getStatistic(): Statistic = object : Statistic {
        override fun getRowCount(): Double = this@CottontailTable.source.statistics.rows.toDouble()
        override fun getDistribution(): RelDistribution = RelDistributions.SINGLETON
        override fun getReferentialConstraints(): MutableList<RelReferentialConstraint> = mutableListOf()
        override fun getCollations(): MutableList<RelCollation> = mutableListOf()
        override fun isKey(columns: ImmutableBitSet?): Boolean = false
    }

    /**
     *
     */
    fun project(root: DataContext, fields: Array<String>): Enumerable<Array<Any?>> = object : AbstractEnumerable<Array<Any?>>() {
        override fun enumerator(): Enumerator<Array<Any?>> = CottontailEntityEnumerator(this@CottontailTable.source, fields, DataContext.Variable.CANCEL_FLAG.get(root))
    }

    /**
     *
     */
    override fun getExpression(schema: SchemaPlus, tableName: String, clazz: Class<*>): Expression {
        return Schemas.tableExpression(schema, this.elementType, tableName, clazz)
    }

    /**
     *
     */
    override fun getElementType(): Type = Array<Any?>::class.java

    /**
     *
     */
    override fun <T : Any?> asQueryable(queryProvider: QueryProvider?, schema: SchemaPlus?, tableName: String?): Queryable<T> {
        throw UnsupportedOperationException()
    }

    /**
     *
     */
    override fun toRel(context: RelOptTable.ToRelContext, relOptTable: RelOptTable): RelNode = CottontailTableScan(context.cluster, relOptTable, relOptTable.rowType.fieldList, this)
}