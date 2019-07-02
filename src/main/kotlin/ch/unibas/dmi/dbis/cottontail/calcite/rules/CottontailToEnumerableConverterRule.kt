package ch.unibas.dmi.dbis.cottontail.calcite.rules

import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailRel

import org.apache.calcite.adapter.enumerable.EnumerableConvention
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.tools.RelBuilderFactory

import java.util.function.Predicate




/**
 * A [ConverterRule] that converts a [CottontailRel] or more exactly a [RelNode] following [CottontailRel.CONVENTION]
 * to a [EnumerableConvention.INSTANCE] using a [CottontailToEnumerableConverter].
 *
 * @see CottontailToEnumerableConverter
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class CottontailToEnumerableConverterRule private constructor(relBuilderFactory: RelBuilderFactory)
    : ConverterRule(RelNode::class.java, Predicate<RelNode> { true }, CottontailRel.CONVENTION, EnumerableConvention.INSTANCE, relBuilderFactory, "CottontailToEnumerableConverterRule") {

    /**
     * Companion object hosting the single instance of [CottontailToEnumerableConverter].
     */
    companion object {
        val INSTANCE: ConverterRule = CottontailToEnumerableConverterRule(RelFactories.LOGICAL_BUILDER)
    }

    /**
     * Converts the given [RelNode] in the [CottontailRel.CONVENTION] to a new [RelNode] following the [EnumerableConvention.INSTANCE].
     */
    override fun convert(rel: RelNode): RelNode {
        val newTraitSet = rel.traitSet.replace(outConvention)
        return CottontailToEnumerableConverter(rel.cluster, newTraitSet, rel)
    }
}