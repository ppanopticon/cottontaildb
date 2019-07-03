package ch.unibas.dmi.dbis.cottontail.calcite.adapter

import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailTableScan
import ch.unibas.dmi.dbis.cottontail.calcite.enumerators.CottontailEntityEnumerator
import ch.unibas.dmi.dbis.cottontail.calcite.enumerators.Enumerators
import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailRel
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException

import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.schema.*
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.schema.Schemas
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.linq4j.*

import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.rel.*
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.QueryProvider
import org.apache.calcite.schema.impl.AbstractTableQueryable
import org.apache.calcite.util.Pair

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
    fun query(fields: List<Pair<String,Class<*>>> = emptyList(), selectFields: List<Pair<String, String>> = emptyList(), where: List<Predicate> = emptyList(), offset: Long = 0, limit: Long = Enumerators.LIMIT_NO_LIMIT): Enumerable<Array<Any?>> = object : AbstractEnumerable<Array<Any?>>() {
        val resolvedFields = fields.map {  Pair(this@CottontailTable.source.columnForName(it.left) ?: throw QueryException.QueryBindException("Failed to bind column '$it' to a column in entity ${this@CottontailTable.source.fqn}."), it.right) }
        val resolvedSelectFields = selectFields.map { Pair(this@CottontailTable.source.columnForName(it.left) ?: throw QueryException.QueryBindException("Failed to bind column '$it' to a column in entity ${this@CottontailTable.source.fqn}."), it.right) }

        override fun enumerator(): Enumerator<Array<Any?>> = CottontailEntityEnumerator(this@CottontailTable.source, resolvedFields, resolvedSelectFields, offset, limit)
    }

    /**
     *
     */
    override fun getExpression(schema: SchemaPlus, tableName: String, clazz: Class<*>): Expression {
        return Schemas.tableExpression(schema, this.elementType, tableName, clazz)
    }

    /**
     * Returns the type of the elements returned by this [CottontailTable].
     */
    override fun getElementType(): Type = Array<Any?>::class.java

    /**
     *
     */
    override fun <T : Any?> asQueryable(queryProvider: QueryProvider, schema: SchemaPlus, tableName: String): Queryable<T> = CottontailTableQueryable(queryProvider, schema, this, tableName)

    /**
     * Transforms this [CottontailTable] to a relational expression of type [CottontailTableScan].
     *
     * @param context [RelOptTable.ToRelContext]
     * @param relOptTable [RelOptTable]
     * @return Resulting [CottontailTableScan]
     */
    override fun toRel(context: RelOptTable.ToRelContext, relOptTable: RelOptTable): RelNode = CottontailTableScan(context.cluster, context.cluster.traitSetOf(CottontailRel.CONVENTION), relOptTable, this, null)


    /**
     *
     */
    class CottontailTableQueryable<T>(queryProvider: QueryProvider, schema: SchemaPlus, table: CottontailTable, tableName: String) : AbstractTableQueryable<T>(queryProvider, schema, table, tableName) {
        /**
         *
         */
        override fun enumerator(): Enumerator<T> = (this.table as CottontailTable).query().enumerator() as Enumerator<T>

        /**
         * Called via code-generation.
         */
        fun query(fields: List<Pair<String,Class<*>>>, selectFields: List<Pair<String, String>>, predicates: List<Predicate>, offset: Long = 0, limit: Long = Enumerators.LIMIT_NO_LIMIT): Enumerable<Array<Any?>> {
            return (this.table as CottontailTable).query(fields, selectFields, predicates, offset, limit)
        }
    }
}