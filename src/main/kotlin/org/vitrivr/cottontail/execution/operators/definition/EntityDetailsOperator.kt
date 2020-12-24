package org.vitrivr.cottontail.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.BooleanValue
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.model.values.StringValue
import kotlin.time.ExperimentalTime

/**
 * An [Operator.SourceOperator] used during query execution. Retrieves [Entity] information.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class EntityDetailsOperator(val catalogue: Catalogue, val name: Name.EntityName) : Operator.SourceOperator() {
    override val columns: Array<ColumnDef<*>> = arrayOf(
            ColumnDef.withAttributes(Name.ColumnName("dbo"), "STRING", -1, false),
            ColumnDef.withAttributes(Name.ColumnName("class"), "STRING", -1, false),
            ColumnDef.withAttributes(Name.ColumnName("type"), "STRING", -1, true),
            ColumnDef.withAttributes(Name.ColumnName("rows"), "INTEGER", -1, true),
            ColumnDef.withAttributes(Name.ColumnName("l_size"), "INTEGER", -1, true),
            ColumnDef.withAttributes(Name.ColumnName("nullable"), "BOOLEAN", -1, true)
    )

    @ExperimentalTime
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val catTxn = context.getTx(this.catalogue) as CatalogueTx
        val schemaTxn = context.getTx(catTxn.schemaForName(this.name.schema())) as SchemaTx
        val entityTxn = context.getTx(schemaTxn.entityForName(this.name)) as EntityTx
        return flow {
            var rowId = 0L
            emit(StandaloneRecord(rowId++,
                    this@EntityDetailsOperator.columns,
                    arrayOf(StringValue(this@EntityDetailsOperator.name.toString()), StringValue("ENTITY"), null, IntValue(entityTxn.count()), null, null)
            ))

            val columns = entityTxn.listColumns()
            columns.forEach {
                emit(StandaloneRecord(rowId++,
                        this@EntityDetailsOperator.columns,
                        arrayOf(StringValue(it.name.toString()), StringValue("COLUMN"), StringValue(it.type.toString()), null, IntValue(it.columnDef.logicalSize), BooleanValue(it.nullable))
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