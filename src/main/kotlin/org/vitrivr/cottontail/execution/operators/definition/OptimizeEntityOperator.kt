package org.vitrivr.cottontail.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * An [Operator.SourceOperator] used during query execution. Optimizes an [Entity]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@ExperimentalTime
class OptimizeEntityOperator(private val catalogue: DefaultCatalogue, private val name: Name.EntityName) : AbstractDataDefinitionOperator(name, "OPTIMIZE ENTITY") {

    override fun toFlow(context: TransactionContext): Flow<Record> {
        val catTxn = context.getTx(this.catalogue) as CatalogueTx
        val schemaTxn = context.getTx(catTxn.schemaForName(this.name.schema())) as SchemaTx
        val entityTxn = context.getTx(schemaTxn.entityForName(this.name)) as EntityTx
        return flow {
            val timedTupleId = measureTimedValue { entityTxn.optimize() }
            emit(this@OptimizeEntityOperator.statusRecord(timedTupleId.duration))
        }
    }
}