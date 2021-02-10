package org.vitrivr.cottontail.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * An [Operator.SourceOperator] used during query execution. Creates an [Entity]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@ExperimentalTime
class CreateEntityOperator(val catalogue: Catalogue, val name: Name.EntityName, val cols: Array<ColumnDef<*>>) : AbstractDataDefinitionOperator(name, "CREATE ENTITY") {

    override fun toFlow(context: TransactionContext): Flow<Record> {
        val catTxn = context.getTx(this.catalogue) as CatalogueTx
        val schemaTxn = context.getTx(catTxn.schemaForName(this.name.schema())) as SchemaTx
        return flow {
            val timedTupleId = measureTimedValue {
                schemaTxn.createEntity(this@CreateEntityOperator.name, *this@CreateEntityOperator.cols)
            }
            emit(this@CreateEntityOperator.statusRecord(timedTupleId.duration))
        }
    }
}