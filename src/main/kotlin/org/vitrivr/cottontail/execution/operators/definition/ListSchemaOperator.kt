package org.vitrivr.cottontail.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.StringValue
import kotlin.time.ExperimentalTime

/**
 * An [Operator.SourceOperator] used during query execution. Lists all available [Schema]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ListSchemaOperator(val catalogue: Catalogue) : Operator.SourceOperator() {
    override val columns: Array<ColumnDef<*>> = arrayOf(
            ColumnDef.withAttributes(Name.ColumnName("dbo"), "STRING", -1, false),
            ColumnDef.withAttributes(Name.ColumnName("class"), "STRING", -1, false)
    )

    @ExperimentalTime
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val txn = context.getTx(this.catalogue) as CatalogueTx
        return flow {
            for (schema in txn.listSchemas()) {
                emit(StandaloneRecord(0L, this@ListSchemaOperator.columns, arrayOf(StringValue(schema.toString()), StringValue("SCHEMA"))))
            }
        }
    }
}