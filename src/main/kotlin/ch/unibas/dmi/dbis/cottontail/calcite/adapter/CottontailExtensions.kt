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
    is StringType -> Entry(this.name.last(), typeFactory.createSqlType(SqlTypeName.VARCHAR))
    is BooleanType -> Entry(this.name.last(), typeFactory.createSqlType(SqlTypeName.BOOLEAN))
    is ByteType -> Entry(this.name.last(), typeFactory.createSqlType(SqlTypeName.TINYINT))
    is ShortType -> Entry(this.name.last(), typeFactory.createSqlType(SqlTypeName.SMALLINT))
    is IntType -> Entry(this.name.last(),typeFactory.createSqlType(SqlTypeName.INTEGER))
    is LongType -> Entry(this.name.last(),typeFactory.createSqlType(SqlTypeName.BIGINT))
    is FloatType -> Entry(this.name.last(),typeFactory.createSqlType(SqlTypeName.FLOAT))
    is DoubleType -> Entry(this.name.last(),typeFactory.createSqlType(SqlTypeName.DOUBLE))
    is IntArrayType -> Entry(this.name.last(),typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), this.size.toLong()))
    is LongArrayType -> Entry(this.name.last(),typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BIGINT), this.size.toLong()))
    is FloatArrayType -> Entry(this.name.last(),typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.FLOAT), this.size.toLong()))
    is DoubleArrayType -> Entry(this.name.last(),typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.DOUBLE), this.size.toLong()))
    is BooleanArrayType -> Entry(this.name.last(),typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BOOLEAN), this.size.toLong()))
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
    is FloatType -> typeFactory.createSqlType(SqlTypeName.FLOAT)
    is DoubleType -> typeFactory.createSqlType(SqlTypeName.DOUBLE)
    is IntArrayType -> typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), Long.MAX_VALUE)
    is LongArrayType -> typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BIGINT), Long.MAX_VALUE)
    is FloatArrayType -> typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.FLOAT), Long.MAX_VALUE)
    is DoubleArrayType -> typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.DOUBLE), Long.MAX_VALUE)
    is BooleanArrayType -> typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BOOLEAN), Long.MAX_VALUE)
}