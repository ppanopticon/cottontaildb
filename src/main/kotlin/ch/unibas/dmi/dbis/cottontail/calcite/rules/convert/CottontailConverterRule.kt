package ch.unibas.dmi.dbis.cottontail.calcite.rules.convert

import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailRel

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.plan.Convention
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.rel.type.RelDataType

import java.util.function.Predicate

/**
 * Abstract base class for all [ConverterRule]s that convert between Apache Calcite's and Cottontail DB's rule sets.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class CottontailConverterRule<T : RelNode>(clazz: Class<T>, description: String, predicate: Predicate<in T> = Predicate { true }) : ConverterRule(clazz, predicate, Convention.NONE, CottontailRel.CONVENTION, RelFactories.LOGICAL_BUILDER, description) {

    /**
     * Static helper methods.
     */
    companion object {

        /**
         * Transforms a [RelDataType] into a list of field names accepted by Cottontail DB
         *
         * @param rowType The [RelDataType] representing the table.
         * @return List of field names.
         */
        fun cottontailFieldNames(rowType: RelDataType): List<String> {
            return SqlValidatorUtil.uniquify(rowType.fieldNames, SqlValidatorUtil.EXPR_SUGGESTER, false)
        }
    }

    /**
     * Reference to the [CottontailRel.COVENTION].
     */
    protected val out: Convention = CottontailRel.CONVENTION
}
