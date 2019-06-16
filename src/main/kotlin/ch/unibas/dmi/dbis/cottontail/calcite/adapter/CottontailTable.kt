package ch.unibas.dmi.dbis.cottontail.calcite.adapter

import ch.unibas.dmi.dbis.cottontail.calcite.expressions.CottontailTableScan
import ch.unibas.dmi.dbis.cottontail.calcite.operations.CottontailEntityEnumerator
import ch.unibas.dmi.dbis.cottontail.calcite.utilities.Entry
import ch.unibas.dmi.dbis.cottontail.database.column.*
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import ch.unibas.dmi.dbis.cottontail.utilities.name.last
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rel.type.RelProtoDataType
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
internal class CottontailTable(private val source: Entity, private val protoRowType: RelProtoDataType? = null) : AbstractTable(), TranslatableTable, QueryableTable {
    /** A cached list of column definitions for apache Calcite. */
    private val columns: MutableList<Entry<String,RelDataType>> = mutableListOf()

    /**
     * Returns the type of the relations returned by this instance of [CottontailTable].
     */
    override fun getRowType(typeFactory: RelDataTypeFactory): RelDataType {
        if (this.protoRowType != null) {
            return this.protoRowType.apply(typeFactory)
        }

        /* Iterates through the columns of the Entity and returns the SQL types. */
        if (this.columns.isEmpty()) {
            this.source.allColumns().forEach {
                this.columns.add(when (it.type) {
                    is StringColumnType -> Entry(it.name.last(), typeFactory.createSqlType(SqlTypeName.VARCHAR))
                    is BooleanColumnType -> Entry(it.name.last(), typeFactory.createSqlType(SqlTypeName.BOOLEAN))
                    is ByteColumnType -> Entry(it.name.last(), typeFactory.createSqlType(SqlTypeName.TINYINT))
                    is ShortColumnType -> Entry(it.name.last(), typeFactory.createSqlType(SqlTypeName.SMALLINT))
                    is IntColumnType -> Entry(it.name.last(),typeFactory.createSqlType(SqlTypeName.INTEGER))
                    is LongColumnType -> Entry(it.name.last(),typeFactory.createSqlType(SqlTypeName.BIGINT))
                    is FloatColumnType -> Entry(it.name.last(),typeFactory.createSqlType(SqlTypeName.FLOAT))
                    is DoubleColumnType -> Entry(it.name.last(),typeFactory.createSqlType(SqlTypeName.DOUBLE))
                    is IntArrayColumnType -> Entry(it.name.last(),typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), it.size.toLong()))
                    is LongArrayColumnType -> Entry(it.name.last(),typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BIGINT), it.size.toLong()))
                    is FloatArrayColumnType -> Entry(it.name.last(),typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.FLOAT), it.size.toLong()))
                    is DoubleArrayColumnType -> Entry(it.name.last(),typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.DOUBLE), it.size.toLong()))
                    is BooleanArrayColumnType -> Entry(it.name.last(),typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BOOLEAN), it.size.toLong()))
                })
            }
        }
        return typeFactory.createStructType(this.columns)
    }

    /**
     * Returns some [Statistics] that describes the [Entity] backing this [CottontailTable].
     *
     * @return [Statistics] regarding the [Entity] that underpins this [CottontailTable].
     */
    override fun getStatistic(): Statistic = object : Statistic {
        override fun getRowCount(): Double = this@CottontailTable.source.statistics.rows.toDouble()
        override fun getReferentialConstraints(): MutableList<RelReferentialConstraint> = mutableListOf()
        override fun getCollations(): MutableList<RelCollation> = mutableListOf()
        override fun isKey(columns: ImmutableBitSet?): Boolean = false
        override fun getDistribution(): RelDistribution = RelDistributions.SINGLETON
    }

    /**
     *
     */
    fun project(root: DataContext, fields: Array<String>): Enumerable<Array<Any?>> {
        val cancelFlag = DataContext.Variable.CANCEL_FLAG.get<AtomicBoolean>(root)
        return object : AbstractEnumerable<Array<Any?>>() {
            override fun enumerator(): Enumerator<Array<Any?>> = CottontailEntityEnumerator(this@CottontailTable.source, fields, cancelFlag)
        }
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
    override fun getElementType(): Type {
        return Array<Any>::class.java
    }

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