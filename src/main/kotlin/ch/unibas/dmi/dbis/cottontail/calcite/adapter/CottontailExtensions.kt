package ch.unibas.dmi.dbis.cottontail.calcite.adapter

import ch.unibas.dmi.dbis.cottontail.calcite.utilities.Entry
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.type.*
import ch.unibas.dmi.dbis.cottontail.utilities.name.last
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.sql.type.SqlTypeName

/**
 * Converts a [ColumnDef] to a [RelDataType].
 *
 * @param typeFactory The [RelDataTypeFactory] to produce the [RelDataType].
 * @return [RelDataType] that corresponds to given [ColumnDef]
 */
fun ColumnDef<*>.toSQLType(typeFactory: RelDataTypeFactory) = when (this.type) {
    is StringType -> Entry(this.name.last(),typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), this.nullable))
    is BooleanType -> Entry(this.name.last(),typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BOOLEAN), this.nullable))
    is ByteType -> Entry(this.name.last(),typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TINYINT), this.nullable))
    is ShortType -> Entry(this.name.last(),typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.SMALLINT), this.nullable))
    is IntType -> Entry(this.name.last(),typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), this.nullable))
    is LongType -> Entry(this.name.last(),typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), this.nullable))
    is FloatType -> Entry(this.name.last(),typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.REAL), this.nullable))
    is DoubleType -> Entry(this.name.last(),typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), this.nullable))
    is IntArrayType -> Entry(this.name.last(),typeFactory.createTypeWithNullability(typeFactory.createArrayType(typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), false), this.size.toLong()), this.nullable))
    is LongArrayType -> Entry(this.name.last(),typeFactory.createTypeWithNullability(typeFactory.createArrayType(typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), false), this.size.toLong()), this.nullable))
    is FloatArrayType -> Entry(this.name.last(),typeFactory.createTypeWithNullability(typeFactory.createArrayType(typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.REAL), false), this.size.toLong()), this.nullable))
    is DoubleArrayType -> Entry(this.name.last(),typeFactory.createTypeWithNullability(typeFactory.createArrayType(typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), false), this.size.toLong()), this.nullable))
    is BooleanArrayType -> Entry(this.name.last(),typeFactory.createTypeWithNullability(typeFactory.createArrayType(typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BOOLEAN), false), this.size.toLong()), this.nullable))
}

/**
 * Converts a [Type] to a [RelDataType].
 *
 * @param typeFactory The [RelDataTypeFactory] to produce the [RelDataType].
 * @return [RelDataType] that corresponds to given [Type]
 */
fun Type<*>.toSQLType(typeFactory: RelDataTypeFactory) : RelDataType = when (this) {
    is StringType -> typeFactory.createSqlType(SqlTypeName.VARCHAR)
    is BooleanType -> typeFactory.createSqlType(SqlTypeName.BOOLEAN)
    is ByteType -> typeFactory.createSqlType(SqlTypeName.TINYINT)
    is ShortType -> typeFactory.createSqlType(SqlTypeName.SMALLINT)
    is IntType -> typeFactory.createSqlType(SqlTypeName.INTEGER)
    is LongType -> typeFactory.createSqlType(SqlTypeName.BIGINT)
    is FloatType -> typeFactory.createSqlType(SqlTypeName.REAL)
    is DoubleType -> typeFactory.createSqlType(SqlTypeName.DOUBLE)
    is IntArrayType -> typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), Long.MAX_VALUE)
    is LongArrayType -> typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BIGINT), Long.MAX_VALUE)
    is FloatArrayType -> typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.REAL), Long.MAX_VALUE)
    is DoubleArrayType -> typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.DOUBLE), Long.MAX_VALUE)
    is BooleanArrayType -> typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BOOLEAN), Long.MAX_VALUE)
}