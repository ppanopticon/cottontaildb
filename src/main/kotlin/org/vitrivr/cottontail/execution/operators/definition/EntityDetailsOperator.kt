package org.vitrivr.cottontail.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.BooleanValue
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.model.values.StringValue
import kotlin.time.ExperimentalTime

/**
 * An [Operator.SourceOperator] used during query execution. Retrieves information regarding entities.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class EntityDetailsOperator(val catalogue: Catalogue, val name: Name.EntityName) : Operator.SourceOperator() {

    companion object {
        val COLUMNS: Array<ColumnDef<*>> = arrayOf(
            ColumnDef(Name.ColumnName("dbo"), Type.String, false),
            ColumnDef(Name.ColumnName("class"), Type.String, false),
            ColumnDef(Name.ColumnName("type"), Type.String, true),
            ColumnDef(Name.ColumnName("rows"), Type.Int, true),
            ColumnDef(Name.ColumnName("l_size"), Type.Int, true),
            ColumnDef(Name.ColumnName("nullable"), Type.Boolean, true)
        )
    }

    override val columns: Array<ColumnDef<*>> = COLUMNS

    @ExperimentalTime
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val catTxn = context.getTx(this.catalogue) as CatalogueTx
        val schemaTxn = context.getTx(catTxn.schemaForName(this.name.schema())) as SchemaTx
        val entityTxn = context.getTx(schemaTxn.entityForName(this.name)) as EntityTx
        return flow {
            var rowId = 0L
            emit(
                StandaloneRecord(
                    rowId++,
                    this@EntityDetailsOperator.columns,
                    arrayOf(
                        StringValue(this@EntityDetailsOperator.name.toString()),
                        StringValue("ENTITY"),
                        null,
                        IntValue(entityTxn.count()),
                        null,
                        null
                    )
                ))

            val columns = entityTxn.listColumns()
            columns.forEach {
                emit(StandaloneRecord(
                    rowId++,
                    this@EntityDetailsOperator.columns,
                    arrayOf(
                        StringValue(it.name.toString()),
                        StringValue("COLUMN"),
                        StringValue(it.type.toString()),
                        null,
                        IntValue(it.columnDef.type.logicalSize),
                        BooleanValue(it.nullable)
                    )
                ))
            }

            val indexes = entityTxn.listIndexes()
            indexes.forEach {
                emit(StandaloneRecord(rowId++,
                        this@EntityDetailsOperator.columns,
                        arrayOf(StringValue(it.name.toString()), StringValue("INDEX"), StringValue(it.type.toString()), null, null, null)
                ))
            }
        }
    }
}