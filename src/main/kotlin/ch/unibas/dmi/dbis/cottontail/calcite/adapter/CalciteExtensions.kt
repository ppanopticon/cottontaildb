package ch.unibas.dmi.dbis.cottontail.calcite.adapter

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.database.queries.*
import ch.unibas.dmi.dbis.cottontail.database.queries.AtomicBooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.CompoundBooleanPredicate
import ch.unibas.dmi.dbis.cottontail.model.type.*
import ch.unibas.dmi.dbis.cottontail.model.values.*

import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.type.SqlTypeName
import java.lang.IllegalArgumentException


/**
 * Maps a [RelDataType] to the corresponding Cottontail DB [Type]
 *
 * @return [Type]
 */
fun RelDataType.toType(): Type<*> = when (this.sqlTypeName) {
    SqlTypeName.BOOLEAN ->  IntType
    SqlTypeName.TINYINT -> ByteType
    SqlTypeName.SMALLINT -> ShortType
    SqlTypeName.INTEGER -> IntType
    SqlTypeName.BIGINT -> LongType
    SqlTypeName.DECIMAL -> DoubleType
    SqlTypeName.FLOAT ->DoubleType
    SqlTypeName.REAL -> FloatType
    SqlTypeName.DOUBLE -> DoubleType
    SqlTypeName.VARCHAR -> StringType
    SqlTypeName.CHAR -> StringType
    else -> throw IllegalArgumentException("The specified SQL type ${this.sqlTypeName} cannot be converted to Cottontail DB type.")
}

/**
 * Maps a [RexLiteral] to the corresponding Cottontail DB [Value]
 *
 * @return [Value]
 */
internal fun RexLiteral.toValue(): Value<*> = when (this.typeName) {
    SqlTypeName.CHAR -> StringValue(RexLiteral.stringValue(this))
    SqlTypeName.VARCHAR -> StringValue(RexLiteral.stringValue(this))
    SqlTypeName.BOOLEAN -> BooleanValue((RexLiteral.value(this) as Boolean))
    SqlTypeName.TINYINT -> ByteValue((RexLiteral.value(this) as Number).toByte())
    SqlTypeName.SMALLINT -> ShortValue((RexLiteral.value(this) as Number).toShort())
    SqlTypeName.INTEGER -> IntValue((RexLiteral.value(this) as Number).toInt())
    SqlTypeName.BIGINT -> LongValue((RexLiteral.value(this) as Number).toLong())
    SqlTypeName.FLOAT -> DoubleValue((RexLiteral.value(this) as Number).toDouble())
    SqlTypeName.DOUBLE -> DoubleValue((RexLiteral.value(this) as Number).toDouble())
    SqlTypeName.DECIMAL -> DoubleValue((RexLiteral.value(this) as Number).toDouble())
    SqlTypeName.REAL -> FloatValue((RexLiteral.value(this) as Number).toFloat())
    else -> throw IllegalArgumentException("The provided value of SQL type ${this.typeName} cannot be converted to a Cottontail DB value.")
}

/**
 * Tries to map a [RexCall] (that must belong to a [LogicalFilter.condition]] to a [Predicate].
 *
 * @param root The [LogicalFilter] this [RexCall] belongs to.
 * @return Corresponding [Predicate] representation.
 */
internal fun RexCall.toPredicate(root: LogicalFilter) : BooleanPredicate {
    assert(root.condition === this)
    return when (this.op) {
        SqlStdOperatorTable.AND, SqlStdOperatorTable.OR, SqlStdOperatorTable.NOT -> CompoundBooleanPredicate(this.op.toConnectionOperator(), *(operands.map { (it as RexCall).toPredicate(root) }.toTypedArray()))
        SqlStdOperatorTable.EQUALS, SqlStdOperatorTable.IN,
        SqlStdOperatorTable.BETWEEN, SqlStdOperatorTable.LIKE,
        SqlStdOperatorTable.GREATER_THAN, SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
        SqlStdOperatorTable.LESS_THAN, SqlStdOperatorTable.LESS_THAN_OR_EQUAL -> {
            val values: List<Value<out Any>> = this.operands.filterIsInstance(RexLiteral::class.java).map { it.toValue() }
            val column: ColumnDef<*> = this.operands.filterIsInstance(RexInputRef::class.java).map {
                val dataType = root.input.rowType.fieldList[it.index]
                ColumnDef(dataType.name, dataType.type.toType(), dataType.type.precision, dataType.type.isNullable)
            }.firstOrNull() ?: throw IllegalArgumentException("Could not convert provided RexCall into a Cottontail DB BooleanPredicate: Failed to find column reference in AtomicPredicate.")
            AtomicBooleanPredicate(column = column, operator = this.op.toComparisonOperator(), values = values)
        }
        SqlStdOperatorTable.NOT_EQUALS, SqlStdOperatorTable.NOT_IN,
        SqlStdOperatorTable.NOT_LIKE, SqlStdOperatorTable.NOT_BETWEEN -> {
            val values: Collection<Value<out Any>> = this.operands.filterIsInstance(RexLiteral::class.java).map { it.toValue() }
            val column: ColumnDef<*> = this.operands.filterIsInstance(RexInputRef::class.java).map {
                val dataType = root.input.rowType.fieldList[it.index]
                ColumnDef(dataType.name, dataType.type.toType(), dataType.type.precision, dataType.type.isNullable)
            }.firstOrNull() ?: throw IllegalArgumentException("Could not convert provided RexCall into a Cottontail DB BooleanPredicate: Failed to find column reference in AtomicPredicate.")
            AtomicBooleanPredicate(column = column, operator = this.op.toComparisonOperator(), values = values)
        }
        else -> throw IllegalArgumentException("Could not convert provided RexCall into a Cottontail DB BooleanPredicate: Encountered unsupported OP (${this.op}).")
    }
}



    /**
     * Maps the standard [SqlOperator]s AND, OR and NOT to a [ConnectionOperator].
     *
     * @return Corresponding [ConnectionOperator].
     */
    internal fun SqlOperator.toConnectionOperator() = when (this) {
        SqlStdOperatorTable.AND -> ConnectionOperator.AND
        SqlStdOperatorTable.OR -> ConnectionOperator.OR
        SqlStdOperatorTable.NOT -> ConnectionOperator.NOT
        else -> throw IllegalArgumentException("The provided SQL operator ${this} cannot be converted to a ConnectionOperator.")
    }

    /**
     * Maps the standard binary [SqlOperator]s (=, !=, <, > etc.) to the corresponding [ComparisonOperator].
     *
     * @return Corresponding [ComparisonOperator].
     */
    internal fun SqlOperator.toComparisonOperator() = when (this) {
        SqlStdOperatorTable.EQUALS -> ComparisonOperator.EQUAL
        SqlStdOperatorTable.NOT_EQUALS -> ComparisonOperator.NOT_EQUAL
        SqlStdOperatorTable.LIKE -> ComparisonOperator.LIKE
        SqlStdOperatorTable.NOT_LIKE -> ComparisonOperator.NOT_LIKE
        SqlStdOperatorTable.BETWEEN -> ComparisonOperator.BETWEEN
        SqlStdOperatorTable.IN -> ComparisonOperator.IN
        SqlStdOperatorTable.NOT_IN -> ComparisonOperator.NOT_IN
        SqlStdOperatorTable.NOT_BETWEEN -> ComparisonOperator.NOT_BETWEEN
        SqlStdOperatorTable.GREATER_THAN -> ComparisonOperator.GREATER
        SqlStdOperatorTable.GREATER_THAN_OR_EQUAL -> ComparisonOperator.GEQUAL
        SqlStdOperatorTable.LESS_THAN -> ComparisonOperator.LESS
        SqlStdOperatorTable.LESS_THAN_OR_EQUAL -> ComparisonOperator.LEQUAL
        SqlStdOperatorTable.IS_NULL -> ComparisonOperator.ISNULL
        SqlStdOperatorTable.IS_NOT_NULL -> ComparisonOperator.ISNOTNULL
        else -> throw IllegalArgumentException("The provided SQL operator ${this} cannot be converted to a ComparisonOperator.")
    }
}