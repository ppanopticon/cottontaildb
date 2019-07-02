package ch.unibas.dmi.dbis.cottontail.calcite.rules

import ch.unibas.dmi.dbis.cottontail.calcite.adapter.CottontailTable
import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailMethod
import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailRel
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import com.google.common.collect.Lists
import org.apache.calcite.adapter.enumerable.*

import org.apache.calcite.plan.ConventionTraitDef
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.AbstractRelNode
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterImpl
import org.apache.calcite.linq4j.tree.BlockBuilder
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.linq4j.tree.Expressions
import org.apache.calcite.util.BuiltInMethod
import org.apache.calcite.util.Pair

import org.apache.calcite.linq4j.tree.MethodCallExpression
import org.apache.calcite.runtime.Hook


/**
* A [ConverterImpl] that facilitates conversion between an output produced under the [CottontailRel.CONVENTION] to an output following the [EnumerableConvention.INSTANCE]
*
* @see CottontailToEnumerableConverter
*
* @author Ralph Gasser
* @version 1.0
*/
internal class CottontailToEnumerableConverter(cluster: RelOptCluster, traits: RelTraitSet, input: RelNode) : ConverterImpl(cluster, ConventionTraitDef.INSTANCE, traits, input), EnumerableRel {

    /**
     * Copies this instance of [CottontailToEnumerableConverter].
     *
     * @param traitSet New [RelTraitSet] to use.
     * @param inputs List of [RelNode]s that act as input.
     * @param [CottontailToEnumerableConverter]
     */
    override fun copy(traitSet: RelTraitSet, inputs: List<RelNode>): RelNode = CottontailToEnumerableConverter(cluster, traitSet, AbstractRelNode.sole(inputs))

    /**
     *
     */
    override fun implement(implementor: EnumerableRelImplementor, pref: EnumerableRel.Prefer): EnumerableRel.Result {

        val list = BlockBuilder()
        val cottontailImplementor = CottontailRel.Implementor()
        cottontailImplementor.visitChild(0, getInput())

        val rowType = getRowType()
        val physType = PhysTypeImpl.of(implementor.typeFactory, rowType, pref.prefer(JavaRowFormat.ARRAY))

        /* Extract list of all fields and transform them into ENUMERABLE convention. */
        val fields = list.append("fields", constantArrayList(CottontailConverterRule.cottontailFieldNames(rowType).mapIndexed { i, v -> Pair(v, physType.fieldClass(i)) }, Pair::class.java))

        /* Extract projections (projected fields + alias) and transform them to ENUMERABLE convention. */
        val selectList = cottontailImplementor.projections.map { Pair(it.key.toString(), it.value) }
        val selectFields = list.append("selectFields", constantArrayList(selectList, Pair::class.java))

        /* */
        val offset = list.append("offset", Expressions.constant(cottontailImplementor.offset))

        /* */
        val limit = list.append("limit", Expressions.constant(cottontailImplementor.limit))

        /* Read table underlying the CottontailRel. */
        val table = list.append("table", cottontailImplementor.table!!.getExpression(CottontailTable.CottontailTableQueryable::class.java))

        /** Read and run predicates. */
        val predicates = list.append("predicates", constantArrayList(cottontailImplementor.predicates, Predicate::class.java))
        Hook.QUERY_PLAN.run(predicates)

        /* Map to enumerable and return. */
        val enumerable = list.append("enumerable", Expressions.call(table, CottontailMethod.COTTONTAIL_QUERYABLE_QUERY.method, fields, selectFields, predicates, offset, limit))
        list.add(Expressions.return_(null, enumerable));
        return implementor.result(physType, list.toBlock());
    }


    /**
     *
     */
    private fun <T> constantArrayList(values: List<T>, clazz: Class<*>): MethodCallExpression =  Expressions.call(BuiltInMethod.ARRAYS_AS_LIST.method, Expressions.newArrayInit(clazz, constantList(values)))

    /**
     *
     */
    private fun <T> constantList(values: List<T>): List<Expression> = Lists.transform(values, Expressions::constant)

}