package org.vitrivr.cottontail.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * An [Operator.SourceOperator] used during query execution. Drops a [Schema]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@ExperimentalTime
class DropSchemaOperator(val catalogue: Catalogue, val name: Name.SchemaName) : AbstractDataDefinitionOperator(name, "DROP SCHEMA") {
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val txn = context.getTx(this.catalogue) as CatalogueTx
        return flow {
            val timedTupleId = measureTimedValue {
                txn.dropSchema(this@DropSchemaOperator.name)
            }
            emit(this@DropSchemaOperator.statusRecord(timedTupleId.duration))
        }
    }
}