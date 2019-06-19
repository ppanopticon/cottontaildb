package ch.unibas.dmi.dbis.cottontail.calcite.knn

import ch.unibas.dmi.dbis.cottontail.calcite.adapter.toSQLType
import ch.unibas.dmi.dbis.cottontail.math.knn.ComparablePair
import ch.unibas.dmi.dbis.cottontail.math.knn.HeapSelect
import ch.unibas.dmi.dbis.cottontail.model.values.DoubleValue
import ch.unibas.dmi.dbis.cottontail.model.values.IntValue
import ch.unibas.dmi.dbis.cottontail.model.values.Value
import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.*
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.RelDistribution
import org.apache.calcite.rel.RelDistributions
import org.apache.calcite.rel.RelReferentialConstraint
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.schema.ScannableTable
import org.apache.calcite.schema.Schema
import org.apache.calcite.schema.Statistic
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.util.ImmutableBitSet

class CottontailKnnTable (backingList: List<HeapSelect<ComparablePair<Value<*>, Double>>>) : AbstractTable(), ScannableTable {

    /** Flattens the backing list into a single list containing all results. However, keeps the relationship between query vector and entry. */
    val list = backingList.mapIndexed { idx, heap -> heap.heap().map { arrayOf(it.first, DoubleValue(it.second), IntValue(idx)) } }.flatten()

    /**
     *
     */
    override fun scan(root: DataContext?): Enumerable<Array<Any>> = object: AbstractEnumerable<Array<Any>>() {
        override fun enumerator(): Enumerator<Array<Any>> = object: TransformedEnumerator<Array<Value<*>>, Array<Any>>(Linq4j.iterableEnumerator(this@CottontailKnnTable.list)) {
            override fun transform(from: Array<Value<*>>?): Array<Any> = from as Array<Any>
        }
    }

    /**
     * Returns the row type of this [CottontailKnnTable]. It always consists of three rows: [id, distance, idx]
     *
     * @return [RelDataType] of this [CottontailKnnTable].
     */
    override fun getRowType(typeFactory: RelDataTypeFactory): RelDataType = typeFactory.createStructType(
        listOf(this.list.first()[0].type.toSQLType(typeFactory), typeFactory.createSqlType(SqlTypeName.DOUBLE), typeFactory.createSqlType(SqlTypeName.INTEGER)),
        listOf("id", "distance", "idx")
    )

    /**
     * Table type of this [CottontailKnnTable].
     */
    override fun getJdbcTableType(): Schema.TableType = Schema.TableType.TEMPORARY_TABLE

    /**
     * Returns [Statistic] for this [CottontailKnnTable].
     *
     * @return The Statistic object.
     */
    override fun getStatistic(): Statistic =  object : Statistic {
        override fun getRowCount(): Double = this@CottontailKnnTable.list.size.toDouble()
        override fun getDistribution(): RelDistribution = RelDistributions.SINGLETON
        override fun getReferentialConstraints(): MutableList<RelReferentialConstraint> = mutableListOf()
        override fun getCollations(): MutableList<RelCollation> = mutableListOf()
        override fun isKey(columns: ImmutableBitSet?): Boolean = false
    }
}