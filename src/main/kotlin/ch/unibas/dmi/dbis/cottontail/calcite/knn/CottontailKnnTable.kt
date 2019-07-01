package ch.unibas.dmi.dbis.cottontail.calcite.knn

import ch.unibas.dmi.dbis.cottontail.calcite.adapter.toSQLType
import ch.unibas.dmi.dbis.cottontail.math.knn.ComparablePair
import ch.unibas.dmi.dbis.cottontail.math.knn.HeapSelect
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

/**
 * A Calcite [Table] implementation that wraps the results of a kNN lookup.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class CottontailKnnTable (backingList: List<HeapSelect<ComparablePair<Value<*>, Double>>>) : AbstractTable(), ScannableTable {

    /** The [Type] of the (first) ID field. */
    private val idType = backingList.first().peek()!!.first.type

    /** Flattens the backing list into a single list containing all results. However, keeps the relationship between query vector and entry. */
    private  val list = backingList.mapIndexed { idx, heap -> (0 until heap.size).map { i -> arrayOf(heap[i].first.value, heap[i].second, idx) } }.flatten()

    /**
     * Implements the scanning operation as required by [ScannableTable].
     *
     * @param root The [DataContext] in which this scanning operation takes place
     */
    override fun scan(root: DataContext): Enumerable<Array<Any>> = object: AbstractEnumerable<Array<Any>>() {
        override fun enumerator(): Enumerator<Array<Any>> = Linq4j.iterableEnumerator(this@CottontailKnnTable.list)
    }

    /**
     * Returns the row type of this [CottontailKnnTable]. It always consists of three rows: [id, distance, idx]
     *
     * @return [RelDataType] of this [CottontailKnnTable].
     */
    override fun getRowType(typeFactory: RelDataTypeFactory): RelDataType = typeFactory.createStructType(
        listOf(this.idType.toSQLType(typeFactory), typeFactory.createSqlType(SqlTypeName.FLOAT), typeFactory.createSqlType(SqlTypeName.INTEGER)),
        listOf("id", "distance", "query")
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